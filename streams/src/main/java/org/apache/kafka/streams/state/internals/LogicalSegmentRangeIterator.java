/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.VersionedRecord;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LogicalSegmentRangeIterator implements KeyValueIterator<Bytes, VersionedRecord<byte[]>> {
    protected final ListIterator<LogicalKeyValueSegment> segmentIterator;
    private final Bytes fromKey;
    private final Bytes toKey;

    private final Long fromTime;
    private final Long toTime;
    // stores the raw value of the latestValueStore when latestValueStore is the current segment
    private KeyValueIterator<Bytes, byte[]> currentKeyValueIterator;
    private ReadonlyPartiallyDeserializedSegmentValue currentValue;
    private Bytes currentKey;
    private boolean isLatest;
    // stores the deserialized value of the current segment (when current segment is one of the old segments)
//    private ReadonlyPartiallyDeserializedSegmentValue currentDeserializedSegmentValue;
    private KeyValue<Bytes, VersionedRecord<byte[]>> next;
    private int nextIndex;

    private volatile boolean open = true;

    // defined for creating/releasing the snapshot.
    private LogicalKeyValueSegment snapshotOwner = null;
    private Snapshot snapshot = null;



    public LogicalSegmentRangeIterator(final ListIterator<LogicalKeyValueSegment> segmentIterator,
                                       final Bytes fromKey,
                                       final Bytes toKey,
                                       final Long fromTime,
                                       final Long toTime) {

        this.segmentIterator = segmentIterator;
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.fromTime = fromTime;
        this.toTime = toTime;
        prepareToFetchNextSegment();
        prepareToFetchNextKeyValue();
    }

    @Override
    public void close() {
        open = false;
        // user may refuse consuming all returned records, so release the snapshot when closing the iterator if it is not released yet!
        releaseSnapshot();
    }

    @Override
    public Bytes peekNextKey() {
        return currentKeyValueIterator.peekNextKey();
    }

    @Override
    public boolean hasNext() {
        if (!open) {
            throw new IllegalStateException("The iterator is out of scope.");
        }
        if (this.next != null) {
            return true;
        }
        if (currentValue == null && currentKeyValueIterator == null && !segmentIterator.hasNext()) {
            return false;
        }

//        while ((currentKeyValueIterator != null || segmentIterator.hasNext()) && this.next == null) {
        while ((currentValue != null || currentKeyValueIterator != null || segmentIterator.hasNext()) && this.next == null) {

            boolean hasSegmentValue = currentValue != null || currentKeyValueIterator != null;
            if (!hasSegmentValue) {
                hasSegmentValue = maybeFillCurrentCurrentKeyValueIterator();
            }
            if (hasSegmentValue) {
                this.next  = getNextRecord();
                if (this.next == null) {
                    prepareToFetchNextKeyValue();
                }
            }
        }
        return this.next != null;
    }

    @Override
    public KeyValue<Bytes, VersionedRecord<byte[]>> next() {
        if (this.next == null) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
        }
        final KeyValue<Bytes, VersionedRecord<byte[]>> clonedNext = next;
        this.next = null;
        return clonedNext;
    }

    private void prepareToFetchNextKeyValue() {
        this.currentKey = null;
        this.currentValue = null;
        this.nextIndex = -1;
    }

    private void prepareToFetchNextSegment() {
        this.currentKeyValueIterator = null;
        this.isLatest = false;
    }

    /**
     * Fills currentRawSegmentValue (for the latestValueStore) or currentDeserializedSegmentValue (for older segments) only if
     * segmentIterator.hasNext() and the segment has records with the query specified key
     */
    private boolean maybeFillCurrentCurrentKeyValueIterator() {
        while (segmentIterator.hasNext()) {
            final LogicalKeyValueSegment segment = segmentIterator.next();

            if (snapshot == null) { // create the snapshot (this will happen only one time).
                // any (random) segment, the latestValueStore or any of the older ones, can be the snapshotOwner, because in
                // fact all use the same physical RocksDB under-the-hood.
                this.snapshotOwner = segment;
                // take a RocksDB snapshot to return the segments content at the query time (in order to guarantee consistency)
                this.snapshot = snapshotOwner.getSnapshot();
            }

            this.currentKeyValueIterator = segment.range(fromKey, toKey, snapshot);
            if (this.currentKeyValueIterator != null) { // this segment contains record(s) with the specified key
                if (segment.id() == -1) {
                    isLatest = true;
                }
                break;
            }
        }
        if (this.currentKeyValueIterator != null) {
            return true;
        }
        // if all segments have been processed, release the snapshot
        releaseSnapshot();
        return false;
    }

    private KeyValue<Bytes, VersionedRecord<byte[]>> getNextRecord() {
        KeyValue<Bytes, VersionedRecord<byte[]>> nextRecord = null;
        if (isLatest) { // this is the latestValueStore
            while (currentKeyValueIterator.hasNext()) {
                final KeyValue<Bytes, byte[]> next = currentKeyValueIterator.next();
                final long recordTimestamp = RocksDBVersionedStore.LatestValueFormatter.getTimestamp(next.value);
                if (recordTimestamp <= toTime) {
                    // latest value satisfies timestamp bound
                    nextRecord = new KeyValue<>(next.key, new VersionedRecord<>(RocksDBVersionedStore.LatestValueFormatter.getValue(next.value), recordTimestamp));
                    break;
                }
            }
        } else {
//            while (currentKeyValueIterator.hasNext()) {
//                final KeyValue<Bytes, byte[]> next = currentKeyValueIterator.next();
//                this.currentValue = new ReadonlyPartiallyDeserializedSegmentValue(next.value);
//                this.currentKey = next.key;
//            }
            if (this.currentValue == null && currentKeyValueIterator != null && currentKeyValueIterator.hasNext()) {
                final KeyValue<Bytes, byte[]> next = currentKeyValueIterator.next();
                this.currentValue = new ReadonlyPartiallyDeserializedSegmentValue(next.value);
                this.currentKey = next.key;
            }
            if (this.currentValue != null) {
                final RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult currentRecord =
                        currentValue.find(fromTime, toTime, ResultOrder.ANY, nextIndex);
                if (currentRecord != null) {
                    nextRecord = new KeyValue<>(this.currentKey, new VersionedRecord<>(currentRecord.value(), currentRecord.validFrom(), currentRecord.validTo()));
                    this.nextIndex = currentRecord.index() + 1;
                }
            }
            if (nextRecord == null || !canSegmentHaveMoreRelevantRecords(nextRecord.value.timestamp())) {
                prepareToFetchNextKeyValue();
            }
        }
        if (currentKeyValueIterator!= null && !currentKeyValueIterator.hasNext()) {
            prepareToFetchNextSegment();
        }
//        // todo no relevant record can be found in the segment
//        if (nextRecord == null || (currentValue != null && )) {
//            prepareToFetchNextKeyValue();
//        }
//        if (!currentKeyValueIterator.hasNext()) {
//            this.currentKeyValueIterator = null;
//        }
        return nextRecord;
    }

    private boolean canSegmentHaveMoreRelevantRecords(final long currentValidFrom) {
        final boolean isCurrentOutsideTimeRange = currentValidFrom <= fromTime || currentValue.minTimestamp() == currentValidFrom;
        return !isCurrentOutsideTimeRange;
    }

    private void releaseSnapshot() {
        if (snapshot != null) {
            snapshotOwner.releaseSnapshot(snapshot);
            snapshot = null;
        }
    }
}

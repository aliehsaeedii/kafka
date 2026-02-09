/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.query.Position;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchInterface;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;

public class DualColumnFamilyAccessor implements RocksDBStore.ColumnFamilyAccessor {
    private final ColumnFamilyHandle oldColumnFamily;
    private final ColumnFamilyHandle newColumnFamily;
    private Position position;

    private DualColumnFamilyAccessor(final ColumnFamilyHandle oldColumnFamily,
                                     final ColumnFamilyHandle newColumnFamily) {
      this.oldColumnFamily = oldColumnFamily;
      this.newColumnFamily = newColumnFamily;
    }

    @Override
    public void put(final RocksDBStore.DBAccessor accessor,
                    final byte[] key,
                    final byte[] valueWithTimestamp,
                    Position position) {
      synchronized (position) {
        if (valueWithTimestamp == null) {
          try {
            accessor.delete(oldColumnFamily, key);
          } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while removing key from store " + name, e);
          }
          try {
            accessor.delete(newColumnFamily, key);
          } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while removing key from store " + name, e);
          }
        } else {
          try {
            accessor.delete(oldColumnFamily, key);
          } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while removing key from store " + name, e);
          }
          try {
            accessor.put(newColumnFamily, key, valueWithTimestamp);
            StoreQueryUtils.updatePosition(position, context);
          } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while putting key/value into store " + name, e);
          }
        }
      }
    }

    @Override
    public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                             final WriteBatchInterface batch) throws RocksDBException {
      for (final KeyValue<Bytes, byte[]> entry : entries) {
        Objects.requireNonNull(entry.key, "key cannot be null");
        addToBatch(entry.key.get(), entry.value, batch);
      }
    }

    @Override
    public byte[] get(final RocksDBStore.DBAccessor accessor, final byte[] key) throws RocksDBException {
      return get(accessor, key, Optional.empty());
    }

    @Override
    public byte[] get(final RocksDBStore.DBAccessor accessor, final byte[] key, final ReadOptions readOptions) throws RocksDBException {
      return get(accessor, key, Optional.of(readOptions));
    }

    private byte[] get(final RocksDBStore.DBAccessor accessor, final byte[] key, final Optional<ReadOptions> readOptions) throws RocksDBException {
      final byte[] valueWithTimestamp = readOptions.isPresent() ? accessor.get(newColumnFamily, readOptions.get(), key) : accessor.get(newColumnFamily, key);
      if (valueWithTimestamp != null) {
        return valueWithTimestamp;
      }

      final byte[] plainValue = readOptions.isPresent() ? accessor.get(oldColumnFamily, readOptions.get(), key) : accessor.get(oldColumnFamily, key);
      if (plainValue != null) {
        final byte[] valueWithUnknownTimestamp = convertToTimestampedFormat(plainValue);
        // this does only work, because the changelog topic contains correct data already
        // for other format changes, we cannot take this short cut and can only migrate data
        // from old to new store on put()
        put(accessor, key, valueWithUnknownTimestamp);
        return valueWithUnknownTimestamp;
      }

      return null;
    }

    @Override
    public byte[] getOnly(final RocksDBStore.DBAccessor accessor, final byte[] key) throws RocksDBException {
      final byte[] valueWithTimestamp = accessor.get(newColumnFamily, key);
      if (valueWithTimestamp != null) {
        return valueWithTimestamp;
      }

      final byte[] plainValue = accessor.get(oldColumnFamily, key);
      if (plainValue != null) {
        return convertToTimestampedFormat(plainValue);
      }

      return null;
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> range(final RocksDBStore.DBAccessor accessor,
                                                        final Bytes from,
                                                        final Bytes to,
                                                        final boolean forward) {
      return new RocksDBTimestampedStore.RocksDBDualCFRangeIterator(
          name,
          accessor.newIterator(newColumnFamily),
          accessor.newIterator(oldColumnFamily),
          from,
          to,
          forward,
          true);
    }

    @Override
    public void deleteRange(final RocksDBStore.DBAccessor accessor, final byte[] from, final byte[] to) {
      try {
        accessor.deleteRange(oldColumnFamily, from, to);
      } catch (final RocksDBException e) {
        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
        throw new ProcessorStateException("Error while removing key from store " + name, e);
      }
      try {
        accessor.deleteRange(newColumnFamily, from, to);
      } catch (final RocksDBException e) {
        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
        throw new ProcessorStateException("Error while removing key from store " + name, e);
      }
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> all(final RocksDBStore.DBAccessor accessor, final boolean forward) {
      final RocksIterator innerIterWithTimestamp = accessor.newIterator(newColumnFamily);
      final RocksIterator innerIterNoTimestamp = accessor.newIterator(oldColumnFamily);
      if (forward) {
        innerIterWithTimestamp.seekToFirst();
        innerIterNoTimestamp.seekToFirst();
      } else {
        innerIterWithTimestamp.seekToLast();
        innerIterNoTimestamp.seekToLast();
      }
      return new RocksDBTimestampedStore.RocksDBDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp, forward);
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final RocksDBStore.DBAccessor accessor, final Bytes prefix) {
      final Bytes to = incrementWithoutOverflow(prefix);
      return new RocksDBTimestampedStore.RocksDBDualCFRangeIterator(
          name,
          accessor.newIterator(newColumnFamily),
          accessor.newIterator(oldColumnFamily),
          prefix,
          to,
          true,
          false
      );
    }

    @Override
    public long approximateNumEntries(final RocksDBStore.DBAccessor accessor) throws RocksDBException {
      return accessor.approximateNumEntries(oldColumnFamily) +
          accessor.approximateNumEntries(newColumnFamily);
    }

    @Override
    public void commit(final RocksDBStore.DBAccessor accessor,
                       final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
      accessor.flush(oldColumnFamily, newColumnFamily);
    }

    @Override
    public void addToBatch(final byte[] key,
                           final byte[] value,
                           final WriteBatchInterface batch) throws RocksDBException {
      if (value == null) {
        batch.delete(oldColumnFamily, key);
        batch.delete(newColumnFamily, key);
      } else {
        batch.delete(oldColumnFamily, key);
        batch.put(newColumnFamily, key, value);
      }
    }

    @Override
    public void close() {
      oldColumnFamily.close();
      newColumnFamily.close();
    }
  }
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

import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A persistent key-(value-timestamp-headers) store based on RocksDB.
 *
 * This is analogous to {@link RocksDBTimestampedStore}, but the "new" column family stores
 * a header-aware format. Legacy values (without headers) are converted on the fly using
 * {@link HeadersBytesStore#convertToHeaderFormat(byte[])}.
 */
public class RocksDBTimestampedStoreWithHeaders extends RocksDBStore implements HeadersBytesStore {

    private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStoreWithHeaders.class);

    // Legacy column family name - must match RocksDBTimestampedStore.TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME
    private static final byte[] LEGACY_TIMESTAMPED_CF_NAME =
        "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8);

    // New column family for header-aware timestamped values.
    private static final byte[] TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME =
        "keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8);

    public RocksDBTimestampedStoreWithHeaders(final String name,
                                              final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBTimestampedStoreWithHeaders(final String name,
                                       final String parentDir,
                                       final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        // We open three CFs:
        //  - DEFAULT_COLUMN_FAMILY: required by RocksDB (not used)
        //  - LEGACY_TIMESTAMPED_CF_NAME: legacy timestamped values (without headers)
        //  - TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME: new header-aware format
        //
        // On first open with no legacy data, we just use the new CF.
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(LEGACY_TIMESTAMPED_CF_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME, columnFamilyOptions)
        );

        final ColumnFamilyHandle defaultCf = columnFamilies.get(0);
        final ColumnFamilyHandle legacyCf = columnFamilies.get(1);
        final ColumnFamilyHandle headersCf = columnFamilies.get(2);

        // Close the default CF as we don't use it
        defaultCf.close();

        final RocksIterator legacyIter = db.newIterator(legacyCf);
        legacyIter.seekToFirst();
        if (legacyIter.isValid()) {
            log.info("Opening store {} in upgrade mode (legacy timestamped -> header-aware timestamped)", name);
            cfAccessor = new DualColumnFamilyAccessor(
                legacyCf,
                headersCf,
                HeadersBytesStore::convertToHeaderFormat,
                this
            );
        } else {
            log.info("Opening store {} in regular header-aware mode", name);
            cfAccessor = new SingleColumnFamilyAccessor(headersCf);
            legacyCf.close();
        }
        legacyIter.close();
    }

}
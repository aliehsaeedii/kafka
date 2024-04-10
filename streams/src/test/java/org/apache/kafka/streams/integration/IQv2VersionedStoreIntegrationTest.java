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
package org.apache.kafka.streams.integration;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.MultiVersionedRangeQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class IQv2VersionedStoreIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String STORE_NAME = "versioned-store";
    private static final Duration HISTORY_RETENTION = Duration.ofDays(1);
    private static final Duration SEGMENT_INTERVAL = Duration.ofHours(1);

    private static final int FIRST_KEY = 0;
    private static final Integer[] RECORD_KEYS = {1, 2, 3};
    private static final int NON_EXISTING_KEY = 3333;

    private static final Instant BASE_TIMESTAMP = Instant.parse("2023-01-01T10:00:00.00Z");
    private static final Long BASE_TIMESTAMP_LONG = BASE_TIMESTAMP.getLong(ChronoField.INSTANT_SECONDS);
    private static final Integer[] RECORD_VALUES = {2, 20, 200, 2000};
    private static final Long[] RECORD_TIMESTAMPS = {BASE_TIMESTAMP_LONG, BASE_TIMESTAMP_LONG + 10, BASE_TIMESTAMP_LONG + 20, BASE_TIMESTAMP_LONG + 30};
    private static final int RECORD_NUMBER = RECORD_VALUES.length;
    private static final int LAST_INDEX = RECORD_NUMBER - 1;
    private static final Position INPUT_POSITION = Position.emptyPosition();

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "true")));

    public static int lastOffset;
    private KafkaStreams kafkaStreams;

    @BeforeClass
    public static void before() throws Exception {
        CLUSTER.start();

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        lastOffset = -1;
        try (final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            for (int j = 0; j < RECORD_VALUES.length; j++) {
                producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0, RECORD_TIMESTAMPS[j], FIRST_KEY, RECORD_VALUES[j])).get();
                lastOffset++;
            }
            for (final Integer recordKey : RECORD_KEYS) {
                for (int j = 0; j < RECORD_VALUES.length; j++) {
                    producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0, RECORD_TIMESTAMPS[j], recordKey, RECORD_VALUES[j])).get();
                    lastOffset++;
                }
            }
        }
        INPUT_POSITION.withComponent(INPUT_TOPIC_NAME, 0, lastOffset);
    }

    @Before
    public void beforeTest() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME,
            Materialized.as(Stores.persistentVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION, SEGMENT_INTERVAL)));
        final Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        kafkaStreams = IntegrationTestUtils.getStartedStreams(configs, builder, true);
    }

    @After
    public void afterTest() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }

    @Test
    public void verifyStore() {
        /* Test Versioned Key Queries */
        // retrieve the latest value
        shouldHandleVersionedKeyQuery(Optional.empty(), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        shouldHandleVersionedKeyQuery(Optional.of(Instant.now()), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        shouldHandleVersionedKeyQuery(Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[3])), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        // retrieve the old value
        shouldHandleVersionedKeyQuery(Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0])), RECORD_VALUES[0], RECORD_TIMESTAMPS[0], Optional.of(RECORD_TIMESTAMPS[1]));
        // there is no record for the provided timestamp
        shouldVerifyGetNullForVersionedKeyQuery(RECORD_KEYS[0], Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50));
        // there is no record with this key
        shouldVerifyGetNullForVersionedKeyQuery(NON_EXISTING_KEY, Instant.now());

        /* Test Multi Versioned Key Queries */
        // retrieve all existing values
        shouldHandleMultiVersionedKeyQuery(Optional.empty(), Optional.empty(), ResultOrder.ANY, 0, LAST_INDEX);
        // retrieve all existing values in ascending order
        shouldHandleMultiVersionedKeyQuery(Optional.empty(), Optional.empty(), ResultOrder.ASCENDING, 0, LAST_INDEX);
        // retrieve existing values in query defined time range
        shouldHandleMultiVersionedKeyQuery(Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[1] + 5)), Optional.of(Instant.now()),
                                           ResultOrder.ANY, 1, LAST_INDEX);
        // there is no record in the query specified time range
        shouldVerifyGetNullForMultiVersionedKeyQuery(RECORD_KEYS[0],
                                                     Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)),
                                                     ResultOrder.ANY);
        // there is no record in the query specified time range even retrieving results in ascending order
        shouldVerifyGetNullForMultiVersionedKeyQuery(RECORD_KEYS[0],
                                                     Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)),
                                                     ResultOrder.ASCENDING);
        // there is no record with this key
        shouldVerifyGetNullForMultiVersionedKeyQuery(NON_EXISTING_KEY, Optional.empty(), Optional.empty(), ResultOrder.ANY);
        // there is no record with this key even retrieving results in ascending order
        shouldVerifyGetNullForMultiVersionedKeyQuery(NON_EXISTING_KEY, Optional.empty(), Optional.empty(), ResultOrder.ASCENDING);
        // test concurrent write while retrieving records
        shouldHandleRaceConditionForMultiVersionedKeyQuery(FIRST_KEY, 0, LAST_INDEX);

        /* Test Multi Versioned Range Queries */
        shouldHandleMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 1), Optional.empty(), Optional.empty(), 0, LAST_INDEX, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 1), Optional.empty(), Optional.empty(), 0, LAST_INDEX, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(1), Optional.of(RECORD_KEYS.length - 1), Optional.empty(), Optional.empty(), 0, LAST_INDEX, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 2), Optional.empty(), Optional.empty(), 0, LAST_INDEX, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 1),  Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[1])), Optional.empty(), 1, LAST_INDEX, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 1), Optional.empty(), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[LAST_INDEX - 1])), 0, LAST_INDEX - 1, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(1), Optional.of(RECORD_KEYS.length - 2),
                Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[1])), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[LAST_INDEX - 1])),
                1, LAST_INDEX - 1, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 1),
                Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[1])), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[LAST_INDEX - 1])),
                1, LAST_INDEX - 1, false);
        shouldHandleMultiVersionedRangeQuery(Optional.of(1), Optional.of(RECORD_KEYS.length - 2),Optional.empty(), Optional.empty(), 0, LAST_INDEX, false);
        // no lower
        shouldHandleMultiVersionedRangeQuery(Optional.empty(), Optional.of(RECORD_KEYS.length - 2),Optional.empty(), Optional.empty(), 0, LAST_INDEX, true);
//        // no upper
//        shouldHandleMultiVersionedRangeQuery(Optional.of(1), Optional.empty(),Optional.empty(), Optional.empty(), 0, LAST_INDEX);
//        // nothing
//        shouldHandleMultiVersionedRangeQuery(Optional.empty(), Optional.empty(),Optional.empty(), Optional.empty(), 0, LAST_INDEX);
        // there is no record in the query specified time range
        shouldVerifyGetNullForMultiVersionedRangeQuery(0, RECORD_KEYS.length - 1,
                                                       Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)));
        // there is no record with the specified key range
        shouldVerifyGetNullForMultiVersionedRangeQuery(NON_EXISTING_KEY, NON_EXISTING_KEY + 1, Optional.empty(), Optional.empty());

        // test concurrent write while retrieving records
        shouldHandleRaceConditionForMultiVersionedRangeQuery(Optional.of(0), Optional.of(RECORD_KEYS.length - 1), 0, LAST_INDEX);
    }


    private void shouldHandleVersionedKeyQuery(final Optional<Instant> queryTimestamp,
                                               final Integer expectedValue,
                                               final Long expectedTimestamp,
                                               final Optional<Long> expectedValidToTime) {

        final VersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEYS[0], queryTimestamp);

        final QueryResult<VersionedRecord<Integer>> queryResult = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        if (queryResult == null) {
            throw new AssertionError("The query returned null.");
        }
        if (queryResult.isFailure()) {
            throw new AssertionError(queryResult.toString());
        }
        if (queryResult.getResult() == null) {
            throw new AssertionError("The query returned null.");
        }

        assertThat(queryResult.isSuccess(), is(true));
        final VersionedRecord<Integer> result1 = queryResult.getResult();
        assertThat(result1.value(), is(expectedValue));
        assertThat(result1.timestamp(), is(expectedTimestamp));
        assertThat(result1.validTo(), is(expectedValidToTime));
        assertThat(queryResult.getExecutionInfo(), is(empty()));
    }

    private void shouldVerifyGetNullForVersionedKeyQuery(final Integer key, final Instant queryTimestamp) {
        final VersionedKeyQuery<Integer, Integer> query = defineQuery(key, Optional.of(queryTimestamp));
        assertThat(sendRequestAndReceiveResults(query, kafkaStreams), nullValue());
    }

    private void shouldHandleMultiVersionedKeyQuery(final Optional<Instant> fromTime, final Optional<Instant> toTime,
                                                    final ResultOrder order, final int expectedArrayLowerBound, final int expectedArrayUpperBound) {

        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEYS[0], fromTime, toTime, order);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            verifyPartitionResult(partitionResultsEntry.getValue());
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = order.equals(ResultOrder.ASCENDING) ? 0 : expectedArrayUpperBound;
                int iteratorSize = 0;
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = i < expectedArrayUpperBound ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    i = order.equals(ResultOrder.ASCENDING) ? i + 1 : i - 1;
                    iteratorSize++;
                }
                // The number of returned records by query is equal to expected number of records
                assertThat(iteratorSize, equalTo(expectedArrayUpperBound - expectedArrayLowerBound + 1));
            }
        }
    }

    private void shouldVerifyGetNullForMultiVersionedKeyQuery(final Integer key, final Optional<Instant> fromTime, final Optional<Instant> toTime, final ResultOrder order) {
        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(key, fromTime, toTime, order);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                assertFalse(iterator.hasNext());
            }
        }
    }

    /**
     * This method updates a record value in an existing timestamp, while it is retrieving records.
     * Since IQv2 guarantees snapshot semantics, we expect that the old value is retrieved.
     */
    private void shouldHandleRaceConditionForMultiVersionedKeyQuery(final Integer key, final int expectedArrayLowerBound, final int expectedArrayUpperBound) {
        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(key, Optional.empty(), Optional.empty(), ResultOrder.ANY);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results in two steps
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = expectedArrayUpperBound;
                int iteratorSize = 0;

                // step 1:
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = i < LAST_INDEX ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    i--;
                    iteratorSize++;
                    if (i == 2) {
                        break;
                    }
                }

                // update the value of the oldest record
                updateRecordValue(key, RECORD_TIMESTAMPS[0]);

                // step 2: continue reading records from through the already opened iterator
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = Optional.of(RECORD_TIMESTAMPS[i + 1]);
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    i--;
                    iteratorSize++;
                }

                // The number of returned records by query is equal to expected number of records
                assertThat(iteratorSize, equalTo(expectedArrayUpperBound - expectedArrayLowerBound + 1));
            }
        }
    }

    private void shouldHandleMultiVersionedRangeQuery(final Optional<Integer> lowerKeyBoundIndex, final Optional<Integer> upperKeyBoundIndex,
                                                      final Optional<Instant> fromTime, final Optional<Instant> toTime,
                                                      final int expectedValueArrayLowerBound, final int expectedValueArrayUpperBound,
                                                      final boolean isFirstRecordIncluded) {

        final MultiVersionedRangeQuery<Integer, Integer> query = defineQueryWithKeyIndices(lowerKeyBoundIndex, upperKeyBoundIndex, fromTime, toTime);

        Map<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        final int lowerIndex = lowerKeyBoundIndex.orElse(0);
        final int upperIndex = upperKeyBoundIndex.orElse(LAST_INDEX);
        for (final Entry<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> partitionResultsEntry : partitionResults.entrySet()) {
            verifyPartitionResult(partitionResultsEntry.getValue());
            try (final KeyValueIterator<Integer, VersionedRecord<Integer>> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = expectedValueArrayUpperBound;
                int j = lowerIndex;
                int iteratorSize = 0;
                // verify latest value store
                if (expectedValueArrayUpperBound == LAST_INDEX) {
                    while (iterator.hasNext()) {
                        final KeyValue<Integer, VersionedRecord<Integer>> record = iterator.next();
                        final Integer key = record.key;
                        final Integer value = record.value.value();
                        final Long timestamp = record.value.timestamp();
                        final Optional<Long> validTo = record.value.validTo();

                        assertThat(key, is(RECORD_KEYS[j]));
                        assertThat(value, is(RECORD_VALUES[i]));
                        assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                        assertThat(validTo, is(Optional.empty()));
                        iteratorSize++;
                        j++;
                        if (j > upperIndex) {
                            i = i - 1;
                            j = lowerIndex;
                            break;
                        }
                    }
                }
                // verify older segments
                if (isFirstRecordIncluded) {
                    while (iterator.hasNext()) {
                        final KeyValue<Integer, VersionedRecord<Integer>> record = iterator.next();
                        final Integer key = record.key;
                        final Integer value = record.value.value();
                        final Long timestamp = record.value.timestamp();
                        final Optional<Long> validTo = record.value.validTo();

                        final Optional<Long> expectedValidTo = i < LAST_INDEX ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                        assertThat(key, is(1));
                        assertThat(value, is(RECORD_VALUES[i]));
                        assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                        assertThat(validTo, is(expectedValidTo));
//                    assertThat(queryResult.getExecutionInfo(), is(empty()));
                        iteratorSize++;
                        i = i - 1;
                        if (i == expectedValueArrayLowerBound - 1) {
                            i = Math.min(LAST_INDEX - 1, expectedValueArrayUpperBound);
                            j++;
                        }
                    }
                }
                while (iterator.hasNext()) {
                    final KeyValue<Integer, VersionedRecord<Integer>> record = iterator.next();
                    final Integer key = record.key;
                    final Integer value = record.value.value();
                    final Long timestamp = record.value.timestamp();
                    final Optional<Long> validTo = record.value.validTo();

                    final Optional<Long> expectedValidTo = i < LAST_INDEX ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertThat(key, is(RECORD_KEYS[j]));
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
//                    assertThat(queryResult.getExecutionInfo(), is(empty()));
                    iteratorSize++;
                    i = i - 1;
                    if (i == expectedValueArrayLowerBound - 1) {
                        i = Math.min(LAST_INDEX - 1, expectedValueArrayUpperBound);
                        j++;
                    }
                }
                // The number of returned records by query is equal to expected number of records
                assertThat(iteratorSize, equalTo((upperIndex - lowerIndex + 1) * (expectedValueArrayUpperBound - expectedValueArrayLowerBound + 1)));
            }
        }
    }

    private void shouldVerifyGetNullForMultiVersionedRangeQuery(final Integer fromKey, final Integer toKey, final Optional<Instant> fromTime, final Optional<Instant> toTime) {
        final MultiVersionedRangeQuery<Integer, Integer> query = defineQuery(fromKey, toKey, fromTime, toTime);

        Map<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        for (final Entry<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final KeyValueIterator<Integer, VersionedRecord<Integer>> iterator = partitionResultsEntry.getValue().getResult()) {
                assertFalse(iterator.hasNext());
            }
        }
    }

    private void shouldHandleRaceConditionForMultiVersionedRangeQuery(final Optional<Integer> lowerKeyBoundIndex, final Optional<Integer> upperKeyBoundIndex,
                                                                      final int expectedArrayLowerBound, final int expectedArrayUpperBound) {
        final MultiVersionedRangeQuery<Integer, Integer> query = defineQueryWithKeyIndices(lowerKeyBoundIndex, upperKeyBoundIndex, Optional.empty(), Optional.empty());

        // send request and get the results
        Map<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);



        // verify results
        for (final Entry<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final KeyValueIterator<Integer, VersionedRecord<Integer>> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = expectedArrayUpperBound;
                int j = lowerKeyBoundIndex.orElse(0);
                int iteratorSize = 0;
                // verify latest value store
                while (iterator.hasNext()) {
                    final KeyValue<Integer, VersionedRecord<Integer>> record = iterator.next();
                    final Integer key = record.key;
                    final Integer value = record.value.value();
                    final Long timestamp = record.value.timestamp();
                    final Optional<Long> validTo = record.value.validTo();

                    final Optional<Long> expectedValidTo = Optional.empty();
                    assertThat(key, is(RECORD_KEYS[j]));
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    iteratorSize++;
                    j++;
                    if (j > upperKeyBoundIndex.orElse(LAST_INDEX)) {
                        i--;
                        j = lowerKeyBoundIndex.orElse(0);
                        break;
                    }
                    // update the corresponding value of the key = 3 in the latestValueStore (RECORD_TIMESTAMP[3])
                    updateRecordValue(RECORD_KEYS[upperKeyBoundIndex.orElse(LAST_INDEX)], RECORD_TIMESTAMPS[3]);
                }

                // update the corresponding value of the key = 2 in the oldest segment (RECORD_TIMESTAMP[0])
                updateRecordValue(RECORD_KEYS[lowerKeyBoundIndex.orElse(0)], RECORD_TIMESTAMPS[0]);

                // verify older segments
                while (iterator.hasNext()) {
                    final KeyValue<Integer, VersionedRecord<Integer>> record = iterator.next();
                    final Integer key = record.key;
                    final Integer value = record.value.value();
                    final Long timestamp = record.value.timestamp();
                    final Optional<Long> validTo = record.value.validTo();

                    final Optional<Long> expectedValidTo = i < expectedArrayUpperBound ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertThat(key, is(RECORD_KEYS[j]));
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    iteratorSize++;
                    i--;
                    if (i == -1) {
                        i = expectedArrayUpperBound - 1;
                        j++;
                    }
                }
                // The number of returned records by query is equal to expected number of records
                assertThat(iteratorSize, equalTo((upperKeyBoundIndex.orElse(LAST_INDEX)- lowerKeyBoundIndex.orElse(0) + 1) * (expectedArrayUpperBound - expectedArrayLowerBound + 1)));
            }
        }
    }

    private static VersionedKeyQuery<Integer, Integer> defineQuery(final Integer key, final Optional<Instant> queryTimestamp) {
        VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        if (queryTimestamp.isPresent()) {
            query = query.asOf(queryTimestamp.get());
        }
        return query;
    }

    private static MultiVersionedKeyQuery<Integer, Integer> defineQuery(final Integer key, final Optional<Instant> fromTime, final Optional<Instant> toTime, final ResultOrder order) {
        MultiVersionedKeyQuery<Integer, Integer> query = MultiVersionedKeyQuery.withKey(key);
        if (fromTime.isPresent()) {
            query = query.fromTime(fromTime.get());
        }
        if (toTime.isPresent()) {
            query = query.toTime(toTime.get());
        }
        if (order.equals(ResultOrder.ASCENDING)) {
            query = query.withAscendingTimestamps();
        }
        return query;
    }

    private static MultiVersionedRangeQuery<Integer, Integer> defineQuery(final Integer fromKey, final Integer toKey,
                                                                          final Optional<Instant> fromTime, final Optional<Instant> toTime) {

        MultiVersionedRangeQuery<Integer, Integer> query = MultiVersionedRangeQuery.withKeyRange(fromKey, toKey);

        if (fromTime.isPresent()) {
            query = query.fromTime(fromTime.get());
        }
        if (toTime.isPresent()) {
            query = query.toTime(toTime.get());
        }
        return query;
    }

    private static MultiVersionedRangeQuery<Integer, Integer> defineQueryWithKeyIndices(final Optional<Integer> lowerKeyBoundIndex, final Optional<Integer> upperKeyBoundIndex,
                                                                                        final Optional<Instant> fromTime, final Optional<Instant> toTime) {

        MultiVersionedRangeQuery<Integer, Integer> query;

        final boolean hasLowerKeyBound = lowerKeyBoundIndex.isPresent();
        final boolean hasUpperKeyBound = upperKeyBoundIndex.isPresent();

        if (!hasLowerKeyBound) {
            if (!hasUpperKeyBound) {
                query = MultiVersionedRangeQuery.allKeys();
            } else {
                query = MultiVersionedRangeQuery.withUpperKeyBound(RECORD_KEYS[upperKeyBoundIndex.get()]);
            }
        } else {
            if (!hasUpperKeyBound) {
                query = MultiVersionedRangeQuery.withLowerKeyBound(RECORD_KEYS[lowerKeyBoundIndex.get()]);
            } else {
                query = MultiVersionedRangeQuery.withKeyRange(RECORD_KEYS[lowerKeyBoundIndex.get()], RECORD_KEYS[upperKeyBoundIndex.get()]);
            }
        }

        if (fromTime.isPresent()) {
            query = query.fromTime(fromTime.get());
        }
        if (toTime.isPresent()) {
            query = query.toTime(toTime.get());
        }
        return query;
    }

    private static QueryResult<VersionedRecord<Integer>> sendRequestAndReceiveResults
            (final VersionedKeyQuery<Integer, Integer> query, final KafkaStreams kafkaStreams) {
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<VersionedRecord<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        return result.getOnlyPartitionResult();
    }

    private static Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> sendRequestAndReceiveResults
            (final MultiVersionedKeyQuery<Integer, Integer> query, final KafkaStreams kafkaStreams) {
        final StateQueryRequest<VersionedRecordIterator<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<VersionedRecordIterator<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        return result.getPartitionResults();
    }

    private static Map<Integer, QueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>>> sendRequestAndReceiveResults
            (final MultiVersionedRangeQuery<Integer, Integer> query, final KafkaStreams kafkaStreams) {
        final StateQueryRequest<KeyValueIterator<Integer, VersionedRecord<Integer>>> request =
                StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<KeyValueIterator<Integer, VersionedRecord<Integer>>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        return result.getPartitionResults();
    }

//    private static void verifyPartitionResult(final QueryResult<VersionedRecordIterator<Integer>> result) {
//        assertThat(result.getExecutionInfo(), is(empty()));
//        if (result.isFailure()) {
//            throw new AssertionError(result.toString());
//        }
//        assertThat(result.isSuccess(), is(true));
//        assertThrows(IllegalArgumentException.class, result::getFailureReason);
//        assertThrows(IllegalArgumentException.class, result::getFailureMessage);
//    }

    private static void verifyPartitionResult(final QueryResult<?> result) {
        assertThat(result.getExecutionInfo(), is(empty()));
        if (result.isFailure()) {
            throw new AssertionError(result.toString());
        }
        assertThat(result.isSuccess(), is(true));
        assertThrows(IllegalArgumentException.class, result::getFailureReason);
        assertThrows(IllegalArgumentException.class, result::getFailureMessage);
    }

    // This method updates the corresponding value of the specified key to 999999 in the specified timestamp
    private void updateRecordValue(final Integer key, final Long timestamp) {
        // update the record value at RECORD_TIMESTAMPS[0]
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0, timestamp, key, 999999));
            lastOffset++;
        }
        INPUT_POSITION.withComponent(INPUT_TOPIC_NAME, 0, lastOffset);
        assertThat(INPUT_POSITION, equalTo(Position.emptyPosition().withComponent(INPUT_TOPIC_NAME, 0, lastOffset)));

        // make sure that the new value is picked up by the store
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "foo" + lastOffset);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC_NAME));
            // the last record is the newly added one
            assertThat(consumer.poll(Duration.ofMillis(10000)).count(), equalTo(lastOffset + 1));
            consumer.close(Duration.ZERO);
        }
    }
}
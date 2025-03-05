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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StreamsGroupConfigTest {

    @Test
    public void testConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_CONFIG, Group.GroupType.CLASSIC.name());
        configs.put(StreamsGroupConfig.STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG, 0);
        configs.put(StreamsGroupConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 45000);
        configs.put(StreamsGroupConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, 45000);


        StreamsGroupConfig config = createConfig(configs);

        assertEquals(Group.GroupType.CLASSIC, config.groupType());
        assertEquals(0, config.streamsGroupTopologyEpoch());
        assertEquals(45000, config.StreamsGroupMinSessionTimeoutMs());
        assertEquals(45000, config.StreamsGroupSessionTimeoutMs());
    }

    @Test
    public void testInvalidConfigs() {
        Map<String, Object> configs = new HashMap<>();
        // test for when STREAMS_GROUP_PROTOCOL_CONFIG is neither STREAMS nor CLASSIC
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_CONFIG, Group.GroupType.SHARE.name());
        assertEquals("group.protocol must be either STREAMS or CLASSIC",
            assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();

        // test for when STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG is negative
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_CONFIG, Group.GroupType.STREAMS.name());
        configs.put(StreamsGroupConfig.STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG, -1);
        assertEquals("Invalid value -1 for configuration topology.epoch: Value must be at least 0",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();

        // test for when STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG is negative
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_CONFIG, Group.GroupType.STREAMS.name());
        configs.put(StreamsGroupConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, -1);
        assertEquals("Invalid value -1 for configuration group.streams.min.session.timeout.ms: Value must be at least 0",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();

        // test for when STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG is negative
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_CONFIG, Group.GroupType.STREAMS.name());
        configs.put(StreamsGroupConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, -1);
        assertEquals("Invalid value -1 for configuration group.streams.session.timeout.ms: Value must be at least 0",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();

        // test for when STREAMS_GROUP_PROTOCOL_CONFIG is CLASSIC and STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG is not 0
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_CONFIG, Group.GroupType.CLASSIC.name());
        configs.put(StreamsGroupConfig.STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG, 1);
        assertEquals("topology.epoch must be 0 when group.protocol=classic",
            assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());
    }

    public static StreamsGroupConfig createStreamsGroupConfig(
        Group.GroupType groupType,
        int streamsGroupTopologyEpoch,
        int streamsGroupMinSessionTimeoutMs,
        int streamsGroupSessionTimeoutMs
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsGroupConfig.STREAMS_GROUP_PROTOCOL_DOC, groupType);
        configs.put(StreamsGroupConfig.STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG, streamsGroupTopologyEpoch);
        configs.put(StreamsGroupConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, streamsGroupMinSessionTimeoutMs);
        configs.put(StreamsGroupConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, streamsGroupSessionTimeoutMs);

        return createConfig(configs);
    }

    private static StreamsGroupConfig createConfig(Map<String, Object> configs) {
        return new StreamsGroupConfig(
            new AbstractConfig(Utils.mergeConfigs(Arrays.asList(StreamsGroupConfig.CONFIG_DEF, GroupCoordinatorConfig.GROUP_COORDINATOR_CONFIG_DEF)), configs, false));
    }
}

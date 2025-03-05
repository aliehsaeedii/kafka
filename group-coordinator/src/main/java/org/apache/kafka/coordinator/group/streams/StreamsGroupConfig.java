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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Group.GroupType;

import java.util.List;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class StreamsGroupConfig {
    /** Streams Group Configurations **/

    public static final String STREAMS_GROUP_PROTOCOL_CONFIG = "group.protocol";
    public static final String STREAMS_GROUP_PROTOCOL_DEFAULT = GroupType.CLASSIC.name();
    public static final String STREAMS_GROUP_PROTOCOL_DOC = "A flag which indicates if the new protocol should be used or not. It could be: classic or streams.";

    public static final String STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG = "topology.epoch";
    public static final int STREAMS_GROUP_TOPOLOGY_EPOCH_DEFAULT  = 0;
    public static final String STREAMS_GROUP_TOPOLOGY_EPOCH_DOC = "The epoch of the topology for the streams group. Ignored if group.protocol=classic.";

    public static final String STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG = "group.streams.min.session.timeout.ms";
    public static final int STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT  = 45000;
    public static final String STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum session timeout.";

    public static final String STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG = "group.streams.session.timeout.ms";
    public static final int STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT  = 45000;
    public static final String STREAMS_GROUP_SESSION_TIMEOUT_MS_DOC = "The timeout to detect client failures when using the streams group protocol.";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(STREAMS_GROUP_PROTOCOL_CONFIG, STRING, STREAMS_GROUP_PROTOCOL_DEFAULT, MEDIUM, STREAMS_GROUP_PROTOCOL_DOC)
            .define(STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG, INT, STREAMS_GROUP_TOPOLOGY_EPOCH_DEFAULT, atLeast(0), MEDIUM, STREAMS_GROUP_TOPOLOGY_EPOCH_DOC)
            .define(STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
            .define(STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, INT, STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, STREAMS_GROUP_SESSION_TIMEOUT_MS_DOC);

    private final GroupType groupType;
    private final int streamsGroupTopologyEpoch;
    private final int streamsGroupMinSessionTimeoutMs;
    private final int streamsGroupSessionTimeoutMs;

    public StreamsGroupConfig(AbstractConfig config) {
        groupType = GroupType.parse(config.getString(STREAMS_GROUP_PROTOCOL_CONFIG));
        streamsGroupTopologyEpoch = config.getInt(STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG);
        streamsGroupMinSessionTimeoutMs = config.getInt(STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        streamsGroupSessionTimeoutMs = config.getInt(STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG);
        validate();
    }

    /** Streams group configuration **/
    public GroupType groupType() {
        return groupType;
    }

    public int streamsGroupTopologyEpoch() {
        return streamsGroupTopologyEpoch;
    }

    public int StreamsGroupMinSessionTimeoutMs() {
        return streamsGroupMinSessionTimeoutMs;
    }

    public int StreamsGroupSessionTimeoutMs() {
        return streamsGroupSessionTimeoutMs;
    }

    private void validate() {
        final List<GroupType> expectedGroupTypes = List.of(GroupType.CLASSIC, GroupType.STREAMS);
        Utils.require(expectedGroupTypes.contains(groupType),
                String.format("%s must be either %s or %s",
                    STREAMS_GROUP_PROTOCOL_CONFIG, GroupType.STREAMS.name(), GroupType.CLASSIC.name()));
        Utils.require(streamsGroupTopologyEpoch >= 0,
                String.format("%s must be greater than or equal to 0",
                    STREAMS_GROUP_TOPOLOGY_EPOCH_CONFIG));
        Utils.require(streamsGroupMinSessionTimeoutMs >= 0,
            String.format("%s must be greater than or equal to 0",
                STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        Utils.require(streamsGroupSessionTimeoutMs >= 0,
            String.format("%s must be greater than or equal to 0",
                STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG));
        if (groupType == GroupType.CLASSIC) {
            Utils.require(streamsGroupTopologyEpoch == 0,
                "topology.epoch must be 0 when group.protocol=classic");
        }
    }
}

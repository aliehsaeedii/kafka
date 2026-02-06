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
package org.apache.kafka.streams.state;


import org.apache.kafka.common.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Marker interface to indicate that a bytes store understands the value-with-headers format
 * and can convert legacy "plain value" entries to the new format.
 * <p>
 * Per the KIP, the value format is: [header_length(varint)][headers_bytes][payload_bytes]
 * where payload_bytes is the existing serialized value (e.g., [timestamp(8)][value] for timestamped stores).
 */
public interface HeadersBytesStore {

    /**
     * Converts a legacy value (without headers) to the header-embedded format.
     * <p>
     * For timestamped stores, the legacy format is: [timestamp(8)][value]
     * The new format is: [header_length(2)][headers][timestamp(8)][value]
     * <p>
     * This method adds empty headers to the existing value format.
     *
     * @param value the legacy value bytes (for timestamped stores: [timestamp(8)][value])
     * @return the value in header-embedded format with empty headers
     */
    static byte[] convertToHeaderFormat(final byte[] value) {
//        if (value == null) {
//            return null;
//        }
//
//        // Format: [headersSize(varint)][headersBytes][payload]
//        // For empty headers:
//        //   headersSize = varint(1) = [0x02] (ZigZag varint)
//        //   headersBytes = [count(varint) = 0] = [0x00]
//        // Result: [0x02][0x00][payload]
//        return ByteBuffer
//            .allocate(2 + value.length)
//            .put((byte) 0x02)
//            .put((byte) 0x00)
//            .put(value)
//            .array();

        if (value == null) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            // Empty headers serialize to an empty byte array (per HeadersSerializer.serialize())
            final byte[] emptyHeadersBytes = new byte[0];

            // Write format: [headers_size(varint)][headers_bytes][payload]
            ByteUtils.writeVarint(emptyHeadersBytes.length, out);  // headers_size = 0
            // No headers_bytes to write (empty array)
            out.write(value);                                       // payload: [timestamp(8)][value]

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert to header format", e);
        }
    }
}
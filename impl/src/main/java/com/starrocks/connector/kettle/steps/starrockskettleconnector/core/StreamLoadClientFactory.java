/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kettle.steps.starrockskettleconnector.core;

import com.starrocks.connector.kettle.steps.starrockskettleconnector.doris.DorisStreamLoadClient;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksStreamLoadClient;

/**
 * Factory for creating Stream Load clients based on database type.
 */
public class StreamLoadClientFactory {

    private StreamLoadClientFactory() {
        // Utility class, prevent instantiation
    }

    /**
     * Create a Stream Load client for the specified database type.
     * @param type the database type
     * @param config the stream load configuration
     * @return the appropriate StreamLoadClient implementation
     * @throws IllegalArgumentException if the database type is not supported
     */
    public static StreamLoadClient createClient(DatabaseType type, StreamLoadConfig config) {
        switch (type) {
            case STARROCKS:
                return new StarRocksStreamLoadClient(config);
            case DORIS:
                return new DorisStreamLoadClient(config);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + type);
        }
    }
}
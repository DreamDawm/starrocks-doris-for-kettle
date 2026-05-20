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

import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * Interface for Stream Load client operations.
 * Abstracts StarRocks and Doris SDK differences.
 */
public interface StreamLoadClient {

    /**
     * Set the log channel for logging output.
     * @param log the log channel from the calling step
     */
    void setLog(LogChannelInterface log);

    /**
     * Initialize the client.
     * @throws Exception if initialization fails
     */
    void init() throws Exception;

    /**
     * Write data to the target table.
     * @param database target database name
     * @param table target table name
     * @param data serialized data string
     * @throws Exception if write operation fails
     */
    void write(String database, String table, String data) throws Exception;

    /**
     * Flush pending data.
     * @throws Exception if flush operation fails
     */
    void flush() throws Exception;

    /**
     * Close the client and release resources.
     * @throws Exception if close operation fails
     */
    void close() throws Exception;

    /**
     * Get any exception that occurred during async operations.
     * @return the exception if any, null otherwise
     */
    Exception getException();

    /**
     * Get the database type this client supports.
     * @return the database type
     */
    DatabaseType getDatabaseType();
}
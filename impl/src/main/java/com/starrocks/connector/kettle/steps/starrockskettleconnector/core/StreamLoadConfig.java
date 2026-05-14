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

import java.util.List;
import java.util.Map;

/**
 * Unified configuration for Stream Load operations.
 * Works for both StarRocks and Doris.
 */
public class StreamLoadConfig {

    private DatabaseType databaseType;
    private List<String> httpUrls;
    private String jdbcUrl;
    private String database;
    private String table;
    private String username;
    private String password;
    private String format;
    private String columnSeparator;
    private String jsonPaths;
    private long maxBytes;
    private float maxFilterRatio;
    private int connectTimeout;
    private int timeout;
    private long scanningFrequency;
    private int ioThreadCount;
    private long chunkLimit;
    private int waitForContinueTimeout;
    private boolean enableUpsertDelete;
    private boolean partialUpdate;
    private String[] columns;
    private String version;
    private Map<String, String> headerProperties;
    private long expectDelayTime;

    private StreamLoadConfig() {
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public List<String> getHttpUrls() {
        return httpUrls;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getFormat() {
        return format;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public float getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getTimeout() {
        return timeout;
    }

    public long getScanningFrequency() {
        return scanningFrequency;
    }

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public long getChunkLimit() {
        return chunkLimit;
    }

    public int getWaitForContinueTimeout() {
        return waitForContinueTimeout;
    }

    public boolean isEnableUpsertDelete() {
        return enableUpsertDelete;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public String[] getColumns() {
        return columns;
    }

    public String getVersion() {
        return version;
    }

    public Map<String, String> getHeaderProperties() {
        return headerProperties;
    }

    public long getExpectDelayTime() {
        return expectDelayTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final StreamLoadConfig config = new StreamLoadConfig();

        public Builder databaseType(DatabaseType databaseType) {
            config.databaseType = databaseType;
            return this;
        }

        public Builder httpUrls(List<String> httpUrls) {
            config.httpUrls = httpUrls;
            return this;
        }

        public Builder jdbcUrl(String jdbcUrl) {
            config.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder database(String database) {
            config.database = database;
            return this;
        }

        public Builder table(String table) {
            config.table = table;
            return this;
        }

        public Builder username(String username) {
            config.username = username;
            return this;
        }

        public Builder password(String password) {
            config.password = password;
            return this;
        }

        public Builder format(String format) {
            config.format = format;
            return this;
        }

        public Builder columnSeparator(String columnSeparator) {
            config.columnSeparator = columnSeparator;
            return this;
        }

        public Builder jsonPaths(String jsonPaths) {
            config.jsonPaths = jsonPaths;
            return this;
        }

        public Builder maxBytes(long maxBytes) {
            config.maxBytes = maxBytes;
            return this;
        }

        public Builder maxFilterRatio(float maxFilterRatio) {
            config.maxFilterRatio = maxFilterRatio;
            return this;
        }

        public Builder connectTimeout(int connectTimeout) {
            config.connectTimeout = connectTimeout;
            return this;
        }

        public Builder timeout(int timeout) {
            config.timeout = timeout;
            return this;
        }

        public Builder scanningFrequency(long scanningFrequency) {
            config.scanningFrequency = scanningFrequency;
            return this;
        }

        public Builder ioThreadCount(int ioThreadCount) {
            config.ioThreadCount = ioThreadCount;
            return this;
        }

        public Builder chunkLimit(long chunkLimit) {
            config.chunkLimit = chunkLimit;
            return this;
        }

        public Builder waitForContinueTimeout(int waitForContinueTimeout) {
            config.waitForContinueTimeout = waitForContinueTimeout;
            return this;
        }

        public Builder waitForContinueTimeoutMs(int waitForContinueTimeoutMs) {
            config.waitForContinueTimeout = waitForContinueTimeoutMs;
            return this;
        }

        public Builder enableUpsertDelete(boolean enableUpsertDelete) {
            config.enableUpsertDelete = enableUpsertDelete;
            return this;
        }

        public Builder partialUpdate(boolean partialUpdate) {
            config.partialUpdate = partialUpdate;
            return this;
        }

        public Builder columns(String[] columns) {
            config.columns = columns;
            return this;
        }

        public Builder columns(String columnsStr) {
            if (columnsStr != null && !columnsStr.isEmpty()) {
                config.columns = columnsStr.split(",");
            }
            return this;
        }

        public Builder version(String version) {
            config.version = version;
            return this;
        }

        public Builder headerProperties(Map<String, String> headerProperties) {
            config.headerProperties = headerProperties;
            return this;
        }

        public Builder expectDelayTime(long expectDelayTime) {
            config.expectDelayTime = expectDelayTime;
            return this;
        }

        public StreamLoadConfig build() {
            return config;
        }
    }
}
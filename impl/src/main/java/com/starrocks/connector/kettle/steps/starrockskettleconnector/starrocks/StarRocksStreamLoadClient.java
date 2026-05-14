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

package com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks;

import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DatabaseType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadClient;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadConfig;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * StarRocks-specific Stream Load client implementation.
 * Wraps the existing StreamLoadManagerV2 from starrocks-stream-load-sdk.
 */
public class StarRocksStreamLoadClient implements StreamLoadClient {

    private StreamLoadManagerV2 streamLoadManager;
    private final StreamLoadConfig config;

    public StarRocksStreamLoadClient(StreamLoadConfig config) {
        this.config = config;
    }

    @Override
    public void init() throws Exception {
        StreamLoadProperties properties = buildStarRocksProperties(config);
        streamLoadManager = new StreamLoadManagerV2(properties, true);
        streamLoadManager.init();
    }

    @Override
    public void write(String database, String table, String data) throws Exception {
        streamLoadManager.write(null, database, table, data);
    }

    @Override
    public void flush() throws Exception {
        if (streamLoadManager != null) {
            streamLoadManager.flush();
        }
    }

    @Override
    public void close() throws Exception {
        if (streamLoadManager != null) {
            streamLoadManager.flush();
            streamLoadManager.close();
            streamLoadManager = null;
        }
    }

    @Override
    public Exception getException() {
        if (streamLoadManager != null) {
            Throwable t = streamLoadManager.getException();
            if (t instanceof Exception) {
                return (Exception) t;
            } else if (t != null) {
                return new RuntimeException(t);
            }
        }
        return null;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.STARROCKS;
    }

    /**
     * Build StreamLoadProperties from StreamLoadConfig for StarRocks SDK.
     */
    private StreamLoadProperties buildStarRocksProperties(StreamLoadConfig config) {
        StreamLoadDataFormat dataFormat;
        if ("CSV".equalsIgnoreCase(config.getFormat())) {
            dataFormat = StreamLoadDataFormat.CSV;
        } else if ("JSON".equalsIgnoreCase(config.getFormat())) {
            dataFormat = StreamLoadDataFormat.JSON;
        } else {
            throw new RuntimeException("Unsupported data format: " + config.getFormat());
        }

        // Build table properties
        StreamLoadTableProperties.Builder tablePropsBuilder = StreamLoadTableProperties.builder()
                .database(config.getDatabase())
                .table(config.getTable())
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(config.getChunkLimit())
                .enableUpsertDelete(config.isEnableUpsertDelete());

        // Set columns if available
        if (config.getColumns() != null && config.getColumns().length > 0) {
            String[] headerColumns;
            if (config.isEnableUpsertDelete() && config.getColumns() != null) {
                headerColumns = new String[config.getColumns().length + 1];
                System.arraycopy(config.getColumns(), 0, headerColumns, 0, config.getColumns().length);
                headerColumns[config.getColumns().length] = "__op";
            } else {
                headerColumns = config.getColumns();
            }
            String cols = Arrays.stream(headerColumns)
                    .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                    .collect(Collectors.joining(","));
            tablePropsBuilder.columns(cols);
        }

        // Build stream load properties map
        Map<String, String> streamLoadProperties = new HashMap<>();
        if (dataFormat instanceof StreamLoadDataFormat.JSONFormat) {
            if (!streamLoadProperties.containsKey("strip_outer_array")) {
                streamLoadProperties.put("strip_outer_array", "true");
            }
            if (!streamLoadProperties.containsKey("ignore_json_size")) {
                streamLoadProperties.put("ignore_json_size", "true");
            }
            if (!streamLoadProperties.containsKey("format")) {
                streamLoadProperties.put("format", "json");
            }
            if (config.getJsonPaths() != null && !config.getJsonPaths().isEmpty()) {
                streamLoadProperties.put("jsonpaths", config.getJsonPaths());
            }
        }

        if (config.isPartialUpdate()) {
            streamLoadProperties.put("partial_update", "true");
        }

        if (config.getHeaderProperties() != null) {
            streamLoadProperties.putAll(config.getHeaderProperties());
        }

        // Build main properties
        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .labelPrefix(config.getDatabaseType().getLabelPrefix())
                .loadUrls(config.getHttpUrls().toArray(new String[0]))
                .jdbcUrl(config.getJdbcUrl())
                .defaultTableProperties(tablePropsBuilder.build())
                .username(config.getUsername())
                .password(config.getPassword())
                .cacheMaxBytes(config.getMaxBytes())
                .ioThreadCount(config.getIoThreadCount())
                .waitForContinueTimeoutMs(config.getWaitForContinueTimeout())
                .scanningFrequency(config.getScanningFrequency())
                .connectTimeout(config.getConnectTimeout())
                .version(config.getVersion())
                .maxRetries(0)
                .expectDelayTime(config.getExpectDelayTime())
                .addHeaders(streamLoadProperties)
                .addHeader("timeout", String.valueOf(config.getTimeout()))
                .addHeader("max_filter_ratio", String.valueOf(config.getMaxFilterRatio()));

        // Add column separator for CSV format
        if (dataFormat instanceof StreamLoadDataFormat.CSVFormat) {
            builder.addHeader("column_separator", config.getColumnSeparator());
        }

        return builder.build();
    }
}
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

package com.starrocks.connector.kettle.steps.starrockskettleconnector.doris;

import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DatabaseType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadClient;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadConfig;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.util.EntityUtils;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Doris-specific Stream Load client implementation.
 * Uses HTTP PUT requests to load data into Doris via Stream Load API.
 */
public class DorisStreamLoadClient implements StreamLoadClient {
    private static final String LOG_CHANNEL_ID = "DorisStreamLoadClient";
    private LogChannelInterface log;

    private final StreamLoadConfig config;
    private CloseableHttpClient httpClient;
    private ScheduledExecutorService scheduler;
    private BlockingQueue<String> dataQueue;
    private final AtomicReference<Exception> asyncException = new AtomicReference<>();
    private volatile boolean running = false;
    private String currentDatabase;
    private String currentTable;
    private long totalRecordsWritten = 0;
    private long lastLoggedRecords = 0;

    public DorisStreamLoadClient(StreamLoadConfig config) {
        this.config = config;
    }

    @Override
    public void setLog(LogChannelInterface log) {
        this.log = log;
    }

    @Override
    public void init() throws Exception {
        if (log == null) {
            log = new LogChannel(LOG_CHANNEL_ID);
        }
        // Use LaxRedirectStrategy to automatically follow 307 redirects for PUT/POST
        httpClient = HttpClients.custom()
                .setMaxConnTotal(config.getIoThreadCount())
                .setMaxConnPerRoute(config.getIoThreadCount())
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build();

        dataQueue = new ArrayBlockingQueue<>(10000);
        scheduler = Executors.newScheduledThreadPool(1);
        running = true;

        // Schedule periodic flush
        scheduler.scheduleAtFixedRate(() -> {
            try {
                flushInternal();
            } catch (Exception e) {
                log.logError("Error during scheduled flush", e);
                asyncException.compareAndSet(null, e);
            }
        }, config.getScanningFrequency(), config.getScanningFrequency(), TimeUnit.MILLISECONDS);

        log.logBasic("Doris Stream Load client initialized for table: " + config.getDatabase() + "." + config.getTable());
    }

    @Override
    public void write(String database, String table, String data) throws Exception {
        if (asyncException.get() != null) {
            throw asyncException.get();
        }

        this.currentDatabase = database != null ? database : config.getDatabase();
        this.currentTable = table != null ? table : config.getTable();

        // Validate database and table names
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            throw new IllegalArgumentException("Database name is not configured. Please set the DATABASE_NAME in the connector configuration.");
        }
        if (this.currentTable == null || this.currentTable.isEmpty()) {
            throw new IllegalArgumentException("Table name is not configured. Please set the TABLE_NAME in the connector configuration.");
        }

        dataQueue.put(data);
        totalRecordsWritten++;

        // Check if we need to flush based on size
        // Simple implementation: flush when queue is half full
        if (dataQueue.size() > 5000) {
            flushInternal();
        }

        // Log progress every 10000 records
        if (totalRecordsWritten - lastLoggedRecords >= 10000) {
            log.logBasic("Written " + totalRecordsWritten + " records to Doris");
            lastLoggedRecords = totalRecordsWritten;
        }
    }

    @Override
    public void flush() throws Exception {
        flushInternal();
    }

    private void flushInternal() throws Exception {
        if (dataQueue.isEmpty()) {
            return;
        }

        List<String> batch = new ArrayList<>();
        dataQueue.drainTo(batch);

        if (batch.isEmpty()) {
            return;
        }

        log.logDetailed("Flushing " + batch.size() + " records to Doris");
        String data = String.join("\n", batch);
        sendStreamLoad(data);
    }

    private void sendStreamLoad(String data) throws Exception {
        String url = buildStreamLoadUrl();
        log.logDetailed("Sending Stream Load to Doris: URL=" + url + ", dataLength=" + data.length());

        HttpPut httpPut = new HttpPut(url);

        // Set authentication
        String auth = config.getUsername() + ":" + config.getPassword();
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);

        // Doris requires Expect: 100-continue header for Stream Load
        httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");

        // Set headers
        httpPut.setHeader("label", generateLabel());
        httpPut.setHeader("format", config.getFormat().toLowerCase());
        httpPut.setHeader("timeout", String.valueOf(config.getTimeout()));
        httpPut.setHeader("max_filter_ratio", String.valueOf(config.getMaxFilterRatio()));

        if ("CSV".equalsIgnoreCase(config.getFormat())) {
            httpPut.setHeader("column_separator", config.getColumnSeparator());
        }

        // Set columns if available - columns are already formatted as `col1`,`col2` string
        if (config.getColumns() != null && config.getColumns().length > 0) {
            // Join the columns array directly (they are already formatted)
            String columns = java.util.Arrays.stream(config.getColumns())
                    .collect(java.util.stream.Collectors.joining(","));
            httpPut.setHeader("columns", columns);
        }

        if (config.getHeaderProperties() != null) {
            for (Map.Entry<String, String> entry : config.getHeaderProperties().entrySet()) {
                httpPut.setHeader(entry.getKey(), entry.getValue());
            }
        }

        httpPut.setEntity(new StringEntity(data, StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            int statusCode = response.getStatusLine().getStatusCode();

            log.logDetailed("Doris Stream Load response: statusCode=" + statusCode);

            if (statusCode != 200) {
                log.logError("Stream Load failed with status " + statusCode + ": " + responseBody);
                throw new RuntimeException("Stream Load failed with status " + statusCode + ": " + responseBody);
            }

            // Check response body for Doris-specific status
            if (responseBody.contains("\"Status\": \"FAIL\"") || responseBody.contains("\"status\": \"FAIL\"")) {
                log.logError("Stream Load failed: " + responseBody);
                throw new RuntimeException("Stream Load failed: " + responseBody);
            }
        } catch (Exception e) {
            log.logError("Stream Load error: " + e.getMessage(), e);
            throw e;
        }
    }

    private String buildStreamLoadUrl() {
        String host = config.getHttpUrls().get(0);
        if (!host.startsWith("http://") && !host.startsWith("https://")) {
            host = "http://" + host;
        }
        return host + "/api/" + currentDatabase + "/" + currentTable + "/_stream_load";
    }

    private String generateLabel() {
        return config.getDatabaseType().getLabelPrefix() + "-" + UUID.randomUUID().toString();
    }

    @Override
    public void close() throws Exception {
        running = false;

        if (scheduler != null) {
            scheduler.shutdown();
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        }

        // Final flush
        flushInternal();

        if (httpClient != null) {
            httpClient.close();
            httpClient = null;
        }

        log.logBasic("Doris Stream Load client closed, total records written: " + totalRecordsWritten);
    }

    @Override
    public Exception getException() {
        return asyncException.get();
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.DORIS;
    }
}
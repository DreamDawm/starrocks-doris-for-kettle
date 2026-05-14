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
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DataType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DatabaseQueryVisitor;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Doris-specific query visitor implementation.
 * Similar to StarRocksQueryVisitor but uses "SELECT version()" for version query.
 */
public class DorisQueryVisitor implements DatabaseQueryVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(DorisQueryVisitor.class);

    private final StarRocksJdbcConnectionProvider jdbcConnProvider;
    private final String database;
    private final String table;

    public DorisQueryVisitor(StarRocksJdbcConnectionProvider jdbcConnProvider, String database, String table) {
        this.jdbcConnProvider = jdbcConnProvider;
        this.database = database;
        this.table = table;
    }

    @Override
    public List<String> getAllTables() throws SQLException, ClassNotFoundException {
        final String query = "select `TABLE_NAME` from `information_schema`.`TABLES` where `TABLE_SCHEMA`=?;";
        List<String> tablenames = new ArrayList<>();
        PreparedStatement stmt = jdbcConnProvider.getConnection().prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, this.database);
        ResultSet rs = stmt.executeQuery();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            tablenames.add(rs.getString(1));
        }
        rs.absolute(currRowIndex);
        rs.close();
        jdbcConnProvider.close();
        return tablenames;
    }

    @Override
    public List<Map<String, Object>> getTableColumnsMetaData() {
        final String query = "select `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?;";
        List<Map<String, Object>> rows;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query, this.database, this.table);
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get table schema info from Doris. " + se.getMessage(), se);
        }
        return rows;
    }

    @Override
    public Map<String, DataType> getFieldMapping() {
        List<Map<String, Object>> columns = getTableColumnsMetaData();

        Map<String, DataType> mapping = new LinkedHashMap<>();
        for (Map<String, Object> column : columns) {
            mapping.put(column.get("COLUMN_NAME").toString(), DataType.fromString(column.get("DATA_TYPE").toString()));
        }

        return mapping;
    }

    @Override
    public String getVersion() {
        // Doris uses "SELECT version()" instead of "SELECT current_version()"
        final String query = "select version() as ver;";
        List<Map<String, Object>> rows;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query);
            if (rows.isEmpty()) {
                return "";
            }
            String version = rows.get(0).get("ver").toString();
            LOG.info(String.format("Doris version: [%s].", version));
            return version;
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get Doris version. " + se.getMessage(), se);
        }
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.DORIS;
    }

    @Override
    public Long getQueryCount(String SQL) {
        Long count = 0L;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", SQL));
            }
            List<Map<String, Object>> data = executeQuery(SQL);
            Object opCount = data.get(0).values().stream().findFirst().orElse(null);
            if (null == opCount) {
                throw new RuntimeException("Failed to get data count from Doris. ");
            }
            count = (Long) opCount;
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get data count from Doris. " + se.getMessage(), se);
        }
        return count;
    }

    private List<Map<String, Object>> executeQuery(String query, String... args) throws ClassNotFoundException, SQLException {
        PreparedStatement stmt = jdbcConnProvider.getConnection().prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        for (int i = 0; i < args.length; i++) {
            stmt.setString(i + 1, args[i]);
        }
        ResultSet rs = stmt.executeQuery();
        rs.next();
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }
        rs.absolute(currRowIndex);
        rs.close();
        jdbcConnProvider.close();
        return list;
    }
}
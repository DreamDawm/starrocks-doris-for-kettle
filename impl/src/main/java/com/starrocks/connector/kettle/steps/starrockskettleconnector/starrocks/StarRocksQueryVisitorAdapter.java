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
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DataType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DatabaseQueryVisitor;
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
 * Adapter wrapping existing StarRocksQueryVisitor to implement DatabaseQueryVisitor interface.
 */
public class StarRocksQueryVisitorAdapter implements DatabaseQueryVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksQueryVisitorAdapter.class);

    private final StarRocksQueryVisitor visitor;
    private final String database;
    private final String table;

    public StarRocksQueryVisitorAdapter(StarRocksQueryVisitor visitor, String database, String table) {
        this.visitor = visitor;
        this.database = database;
        this.table = table;
    }

    @Override
    public List<String> getAllTables() throws SQLException, ClassNotFoundException {
        return visitor.getAllTables();
    }

    @Override
    public List<Map<String, Object>> getTableColumnsMetaData() {
        return visitor.getTableColumnsMetaData();
    }

    @Override
    public Map<String, DataType> getFieldMapping() {
        Map<String, StarRocksDataType> starRocksMapping = visitor.getFieldMapping();
        Map<String, DataType> mapping = new LinkedHashMap<>();
        for (Map.Entry<String, StarRocksDataType> entry : starRocksMapping.entrySet()) {
            mapping.put(entry.getKey(), convertToDataType(entry.getValue()));
        }
        return mapping;
    }

    @Override
    public String getVersion() {
        return visitor.getStarRocksVersion();
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.STARROCKS;
    }

    @Override
    public Long getQueryCount(String sql) {
        return visitor.getQueryCount(sql);
    }

    /**
     * Get the wrapped StarRocksQueryVisitor.
     * @return the wrapped visitor
     */
    public StarRocksQueryVisitor getVisitor() {
        return visitor;
    }

    /**
     * Convert StarRocksDataType to unified DataType.
     */
    private DataType convertToDataType(StarRocksDataType starRocksDataType) {
        if (starRocksDataType == null) {
            return DataType.UNKNOWN;
        }
        return DataType.fromString(starRocksDataType.name());
    }
}
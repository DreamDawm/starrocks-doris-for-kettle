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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Interface for database metadata queries.
 * Abstracts StarRocks and Doris query visitor implementations.
 */
public interface DatabaseQueryVisitor {

    /**
     * Get all tables in the database.
     * @return list of table names
     * @throws SQLException if query fails
     * @throws ClassNotFoundException if JDBC driver not found
     */
    List<String> getAllTables() throws SQLException, ClassNotFoundException;

    /**
     * Get column metadata for the configured table.
     * @return list of column metadata maps
     */
    List<Map<String, Object>> getTableColumnsMetaData();

    /**
     * Get field name to data type mapping for the configured table.
     * @return map of column name to DataType
     */
    Map<String, DataType> getFieldMapping();

    /**
     * Get database version string.
     * @return the version string
     */
    String getVersion();

    /**
     * Get the database type this visitor supports.
     * @return the database type
     */
    DatabaseType getDatabaseType();

    /**
     * Execute a count query.
     * @param sql the SQL query
     * @return the count result
     */
    Long getQueryCount(String sql);
}
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

import java.util.HashMap;
import java.util.Map;

/**
 * Unified data type enum for both StarRocks and Doris.
 * Both databases share the same data types as they originated from the same project.
 */
public enum DataType {
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    LARGEINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    BOOLEAN,
    VARCHAR,
    CHAR,
    STRING,
    JSON,
    DATE,
    DATETIME,
    UNKNOWN;

    private static final Map<String, DataType> dataTypeMap = new HashMap<>();

    static {
        DataType[] dataTypes = DataType.values();
        for (DataType dataType : dataTypes) {
            dataTypeMap.put(dataType.name(), dataType);
        }
    }

    /**
     * Parse a string to DataType.
     * @param typeString the type string (case-insensitive)
     * @return the corresponding DataType, or UNKNOWN if not found
     */
    public static DataType fromString(String typeString) {
        if (typeString == null) {
            return UNKNOWN;
        }

        DataType dataType = dataTypeMap.get(typeString);
        if (dataType == null) {
            dataType = dataTypeMap.getOrDefault(typeString.toUpperCase(), DataType.UNKNOWN);
        }

        return dataType;
    }
}
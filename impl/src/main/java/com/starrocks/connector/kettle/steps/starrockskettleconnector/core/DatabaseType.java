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

/**
 * Enum representing supported database types for the Kettle connector.
 * Currently supports StarRocks and Apache Doris.
 */
public enum DatabaseType {
    STARROCKS("StarRocks"),
    DORIS("Doris");

    private final String displayName;

    DatabaseType(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Get the display name for UI purposes.
     * @return the display name
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Get the label prefix for Stream Load operations.
     * @return the label prefix (e.g., "StarRocks-Kettle" or "Doris-Kettle")
     */
    public String getLabelPrefix() {
        return this.displayName + "-Kettle";
    }

    /**
     * Parse a string to DatabaseType.
     * Defaults to STARROCKS for backward compatibility.
     * @param name the database type name (case-insensitive)
     * @return the corresponding DatabaseType, defaults to STARROCKS if not found
     */
    public static DatabaseType fromString(String name) {
        if (name == null || name.isEmpty()) {
            return STARROCKS; // Default for backward compatibility
        }
        for (DatabaseType type : values()) {
            if (type.displayName.equalsIgnoreCase(name) || type.name().equalsIgnoreCase(name)) {
                return type;
            }
        }
        return STARROCKS;
    }
}
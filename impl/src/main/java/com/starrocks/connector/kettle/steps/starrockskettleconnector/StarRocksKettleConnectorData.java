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

package com.starrocks.connector.kettle.steps.starrockskettleconnector;

import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DataType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.Serializer;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadClient;

import java.util.Map;

/**
 * Stores data for the StarRocks Kettle Connector step.
 */
public class StarRocksKettleConnectorData extends BaseStepData implements StepDataInterface {

    // Stream Load client (supports both StarRocks and Doris)
    public StreamLoadClient streamLoadClient;

    // Serializer for data format conversion
    public Serializer serializer;

    // In StarRocks/Doris, if you want to implement changes to the data and partial imports, you need to add '__op'.
    public String[] columns;

    // The index corresponding to the data type of the row element.
    public int[] keynrs; // nr of keylookup -value in row...

    // The field name and field type of the target table.
    public Map<String, DataType> fieldtype;
    public String tablename;
    public String databasename;


    public StarRocksKettleConnectorData() {
        super();

        streamLoadClient = null;
    }
}

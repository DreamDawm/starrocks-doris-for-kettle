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

import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DatabaseQueryVisitor;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DatabaseType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.DataType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.Serializer;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadClient;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadClientFactory;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.core.StreamLoadConfig;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.doris.DorisQueryVisitor;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksCsvSerializer;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksJsonSerializer;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksQueryVisitor;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksQueryVisitorAdapter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksKettleConnector extends BaseStep implements StepInterface {

    private static Class<?> PKG = StarRocksKettleConnectorMeta.class;
    private StarRocksKettleConnectorMeta meta;
    private StarRocksKettleConnectorData data;
    /**
     * The desired delay time for data refresh。
     * ageThreshold = expectDelayTime / scanFrequency;
     * Calculate the latest submission time。
     */
    private long expectDelayTime = 30000L;

    public StarRocksKettleConnector(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        try {

            Object[] r = getRow(); // Get row from input rowset & set row busy!
            if (r == null) { // no more input to be expected...
                setOutputDone();
                closeOutput();
                return false;
            }

            if (data.streamLoadClient.getException() != null) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Log.AsyncWriteError"), data.streamLoadClient.getException());
                setErrors(1);
                stopAll();
                setOutputDone();
                return false;
            }

            if (first) {
                first = false;

                // Cache field indexes.
                data.keynrs = new int[meta.getFieldStream().length];
                for (int i = 0; i < data.keynrs.length; i++) {
                    data.keynrs[i] = getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
                }
                data.serializer = getSerializer(meta);
            }
            String serializedValue = data.serializer.serialize(transform(r, meta.getEnableUpsertDelete()));
            data.streamLoadClient.write(data.databasename, data.tablename, serializedValue);

            putRow(getInputRowMeta(), r);
            incrementLinesOutput();
            return true;

        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Log.ErrorInStep") + e);
            setErrors(1);
            stopAll();
            setOutputDone();
            return false;
        }
    }

    private void closeOutput() throws Exception {
        data.streamLoadClient.flush();
        data.streamLoadClient.close();
        if (data.streamLoadClient.getException() != null) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), data.streamLoadClient.getException());
        }
        data.streamLoadClient = null;
    }

    // Data type conversion.
    public Object[] transform(Object[] r, boolean supportUpsertDelete) throws KettleException {
        Object[] values = new Object[data.keynrs.length + (supportUpsertDelete ? 1 : 0)];
        for (int i = 0; i < data.keynrs.length; i++) {
            ValueMetaInterface sourceMeta = getInputRowMeta().getValueMeta(data.keynrs[i]);
            DataType dataType = data.fieldtype.get(meta.getFieldTable()[i]);
            values[i] = typeConversion(sourceMeta, dataType, r[i]);
        }
        if (supportUpsertDelete && meta.getUpsertOrDelete() != null && meta.getUpsertOrDelete().length() != 0) {
            values[data.keynrs.length] = StarRocksOP.parse(meta.getUpsertOrDelete()).ordinal();
        }
        return values;
    }

    /**
     * Data type conversion.
     *
     * @param sourceMeta
     * @param type
     * @param r
     * @return
     */
    public Object typeConversion(ValueMetaInterface sourceMeta, DataType type, Object r) throws KettleException {
        if (r == null) {
            return null;
        }
        try {
            switch (sourceMeta.getType()) {
                case ValueMetaInterface.TYPE_STRING:
                    String sValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        sValue = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        sValue = sourceMeta.getString(r);
                    }
                    return sValue;
                case ValueMetaInterface.TYPE_BOOLEAN:
                    Boolean boolenaValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        String binaryBoolean = new String((byte[]) r, StandardCharsets.UTF_8);
                        boolenaValue = binaryBoolean.equals("1") || binaryBoolean.equals("true") || binaryBoolean.equals("True") || binaryBoolean.equals("TRUE");
                    } else {
                        boolenaValue = sourceMeta.getBoolean(r);
                    }
                    return boolenaValue;
                case ValueMetaInterface.TYPE_INTEGER:
                    Long integerValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        integerValue = Long.parseLong(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        integerValue = sourceMeta.getInteger(r);
                    }
                    if (integerValue >= Byte.MIN_VALUE && integerValue <= Byte.MAX_VALUE && type == DataType.TINYINT) {
                        return integerValue.byteValue();
                    } else if (integerValue >= Short.MIN_VALUE && integerValue <= Short.MAX_VALUE && type == DataType.SMALLINT) {
                        return integerValue.shortValue();
                    } else if (integerValue >= Integer.MIN_VALUE && integerValue <= Integer.MAX_VALUE && type == DataType.INT) {
                        return integerValue.intValue();
                    } else {
                        return integerValue;
                    }
                case ValueMetaInterface.TYPE_NUMBER:
                    Double doubleValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        doubleValue = Double.parseDouble(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        doubleValue = sourceMeta.getNumber(r);
                    }
                    return doubleValue;
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    BigDecimal decimalValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        decimalValue = new BigDecimal(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        decimalValue = sourceMeta.getBigNumber(r);
                    }
                    return decimalValue; // BigDecimal string representation is compatible with DECIMAL
                case ValueMetaInterface.TYPE_DATE:
                    SimpleDateFormat sourceDateFormatter = sourceMeta.getDateFormat();
                    SimpleDateFormat dateFormatter = null;
                    if (type == DataType.DATE) {
                        // DATE type format: 'yyyy-MM-dd'
                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    } else {
                        // DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    Date dateValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        String dateStr = new String((byte[]) r, StandardCharsets.UTF_8);
                        dateValue = sourceDateFormatter.parse(dateStr);
                    } else {
                        dateValue = sourceMeta.getDate(r);
                    }

                    return dateFormatter.format(dateValue);
                case ValueMetaInterface.TYPE_TIMESTAMP:
                    SimpleDateFormat sourceTimestampFormatter = sourceMeta.getDateFormat();
                    SimpleDateFormat timeStampFormatter = null;
                    if (type == DataType.DATE) {
                        // DATE type format: 'yyyy-MM-dd'
                        timeStampFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    } else {
                        // DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
                        timeStampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    java.sql.Timestamp timestampValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        String timestampStr = new String((byte[]) r, StandardCharsets.UTF_8);
                        timestampValue = new java.sql.Timestamp(sourceTimestampFormatter.parse(timestampStr).getTime());
                    } else {
                        timestampValue = (Timestamp) sourceMeta.getDate(r);
                    }
                    return timeStampFormatter.format(timestampValue);
                case ValueMetaInterface.TYPE_BINARY:
                    throw new KettleException((BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnSupportBinary") + r.toString()));

                case ValueMetaInterface.TYPE_INET:
                    String address;
                    if (sourceMeta.isStorageBinaryString()) {

                        address = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        address = (String) r;
                    }
                    return address;
                default:
                    throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnknowType") + ValueMetaInterface.getTypeDescription(sourceMeta.getType()));
            }
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConvertType") + e.getMessage());
        }
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        if (super.init(smi, sdi)) {
            // Add columns properties to all to prevent changes in the order of the fields.
            if (meta.getPartialUpdate() && meta.getPartialcolumns() != null && meta.getPartialcolumns().length != 0) {
                data.columns = new String[meta.getPartialcolumns().length];
                System.arraycopy(meta.getPartialcolumns(), 0, data.columns, 0, meta.getPartialcolumns().length);
            } else {
                data.columns = new String[meta.getFieldTable().length];
                System.arraycopy(meta.getFieldTable(), 0, data.columns, 0, meta.getFieldTable().length);
            }
            if (meta.getQueryVisitor() == null) {
                // Used to find field information in database.
                StarRocksJdbcConnectionOptions jdbcConnectionOptions = new StarRocksJdbcConnectionOptions(meta.getJdbcurl(), meta.getUser(), meta.getPassword());
                StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
                DatabaseType dbType = meta.getDatabaseTypeEnum();
                if (dbType == DatabaseType.DORIS) {
                    meta.setQueryVisitor(new DorisQueryVisitor(jdbcConnectionProvider, meta.getDatabasename(), meta.getTablename()));
                } else {
                    StarRocksQueryVisitor srVisitor = new StarRocksQueryVisitor(jdbcConnectionProvider, meta.getDatabasename(), meta.getTablename());
                    meta.setQueryVisitor(new StarRocksQueryVisitorAdapter(srVisitor, meta.getDatabasename(), meta.getTablename()));
                }
            }
            try {
                StreamLoadConfig config = buildStreamLoadConfig(meta, data);
                data.streamLoadClient = StreamLoadClientFactory.createClient(meta.getDatabaseTypeEnum(), config);
                data.streamLoadClient.init();
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConnManager"), e);
                return false;
            }
            try {
                data.fieldtype = meta.getQueryVisitor().getFieldMapping();
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.MissingStarRocksFieldType"));
                return false;
            }
            data.tablename = meta.getTablename();
            data.databasename = meta.getDatabasename();
            return true;
        }
        return false;

    }

    public Serializer getSerializer(StarRocksKettleConnectorMeta meta) {
        Serializer serializer;
        if (meta.getFormat().equals("CSV")) {
            serializer = new StarRocksCsvSerializer(meta.getColumnSeparator());
        } else if (meta.getFormat().equals("JSON")) {
            String[] jsonFileTable;
            if (meta.getEnableUpsertDelete() && meta.getUpsertOrDelete() != null && meta.getUpsertOrDelete().length() != 0) {
                jsonFileTable = new String[data.columns.length + 1];
                System.arraycopy(data.columns, 0, jsonFileTable, 0, data.columns.length);
                jsonFileTable[data.columns.length] = "__op";
            } else {
                jsonFileTable = meta.getFieldTable();
            }
            serializer = new StarRocksJsonSerializer(jsonFileTable);
        } else {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFormat"));
            return null;
        }
        return serializer;
    }

    // Build StreamLoadConfig for Stream Load client.
    public StreamLoadConfig buildStreamLoadConfig(StarRocksKettleConnectorMeta meta, StarRocksKettleConnectorData data) {
        Map<String, String> headerProperties = new HashMap<>();
        // By default, using json format should enable strip_outer_array and ignore_json_size,
        // which will simplify the configurations
        if (meta.getFormat().equalsIgnoreCase("JSON")) {
            if (!headerProperties.containsKey("strip_outer_array")) {
                headerProperties.put("strip_outer_array", "true");
            }
            if (!headerProperties.containsKey("ignore_json_size")) {
                headerProperties.put("ignore_json_size", "true");
            }
            if (!headerProperties.containsKey("format")) {
                headerProperties.put("format", "json");
            }
            if (meta.getJsonpaths() != null && meta.getJsonpaths().length() != 0) {
                headerProperties.put("jsonpaths", meta.getJsonpaths());
            }
        }
        if (meta.getPartialUpdate()) {
            if (!headerProperties.containsKey("partial_update")) {
                headerProperties.put("partial_update", "true");
            }
        }

        if (meta.getHeaderProperties() != null && meta.getHeaderProperties().length() != 0) {
            try {
                String[] properties = meta.getHeaderProperties().split(";");
                for (String property : properties) {
                    String[] parts = property.split(":");
                    if (parts.length >= 2) {
                        headerProperties.put(parts[0], parts[1]);
                    }
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.Exception.UnableProperties"));
            }
        }

        // Build columns header if needed
        String columnsHeader = null;
        if (data.columns != null) {
            // don't need to add "columns" header in following cases
            // 1. use csv format but the kettle and target database schemas are aligned
            // 2. use json format, except that it's loading to a primary key table for older versions
            boolean noNeedAddColumnsHeader;
            if (meta.getFormat().equalsIgnoreCase("CSV")) {
                noNeedAddColumnsHeader = false;
            } else {
                noNeedAddColumnsHeader = !meta.getEnableUpsertDelete() || meta.isOpAutoProjectionInJson();
            }
            if (!noNeedAddColumnsHeader) {
                String[] headerColumns;
                if (meta.getEnableUpsertDelete() && meta.getUpsertOrDelete() != null && meta.getUpsertOrDelete().length() != 0) {
                    headerColumns = new String[data.columns.length + 1];
                    System.arraycopy(data.columns, 0, headerColumns, 0, data.columns.length);
                    headerColumns[data.columns.length] = "__op";
                } else {
                    headerColumns = data.columns;
                }
                columnsHeader = Arrays.stream(headerColumns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
            } else {
                columnsHeader = Arrays.stream(data.columns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
            }
        }

        StreamLoadConfig.Builder builder = StreamLoadConfig.builder()
                .databaseType(meta.getDatabaseTypeEnum())
                .httpUrls(meta.getHttpurl())
                .jdbcUrl(meta.getJdbcurl())
                .database(meta.getDatabasename())
                .table(meta.getTablename())
                .username(meta.getUser())
                .password(meta.getPassword())
                .format(meta.getFormat())
                .columnSeparator(meta.getColumnSeparator())
                .maxBytes(meta.getMaxbytes())
                .maxFilterRatio(meta.getMaxFilterRatio())
                .connectTimeout(meta.getConnecttimeout())
                .timeout(meta.getTimeout())
                .ioThreadCount(meta.getIoThreadCount())
                .scanningFrequency(meta.getScanningFrequency())
                .waitForContinueTimeoutMs(meta.getWaitForContinueTimeout())
                .headerProperties(headerProperties)
                .columns(columnsHeader);

        return builder.build();
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        try {
            if (data.streamLoadClient != null) {
                data.streamLoadClient.flush();
                if (data.streamLoadClient.getException() != null) {
                    logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), data.streamLoadClient.getException());
                }
                data.streamLoadClient.close();
                data.streamLoadClient = null;
            }
        } catch (Exception e) {
            setErrors(1L);
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UNEXPECTEDERRORCLOSING"), e);
        }
        super.dispose(smi, sdi);
    }


}

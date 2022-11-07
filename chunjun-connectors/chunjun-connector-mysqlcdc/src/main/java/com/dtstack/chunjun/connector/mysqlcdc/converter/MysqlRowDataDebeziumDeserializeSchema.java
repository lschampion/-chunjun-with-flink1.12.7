//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.dtstack.chunjun.connector.mysqlcdc.converter;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.Envelope.Operation;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;



public class MysqlRowDataDebeziumDeserializeSchema implements DebeziumDeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private final TypeInformation<RowData> resultTypeInfo;
    private final MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter runtimeConverter;
    private final ZoneId serverTimeZone;
    private final MysqlRowDataDebeziumDeserializeSchema.ValueValidator validator;
//    private MysqlCdcColumnTypeConverter columnTypeConverter=new MysqlCdcColumnTypeConverter();


    public MysqlRowDataDebeziumDeserializeSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, MysqlRowDataDebeziumDeserializeSchema.ValueValidator validator, ZoneId serverTimeZone) {
        this.runtimeConverter = this.createConverter(rowType);
        this.resultTypeInfo = resultTypeInfo;
        this.validator = validator;
        this.serverTimeZone = serverTimeZone;
    }

    public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
        Operation op = Envelope.operationFor(record);
        Struct value = (Struct)record.value();
        Schema valueSchema = record.valueSchema();
        GenericRowData delete;

        initFieldListFull(record);
        if (op != Operation.CREATE && op != Operation.READ) {
            if (op == Operation.DELETE) {
                delete = this.extractBeforeRow(value, valueSchema);
                this.validator.validate(delete, RowKind.DELETE);
                delete.setRowKind(RowKind.DELETE);
                out.collect(delete);
            } else {
                delete = this.extractBeforeRow(value, valueSchema);
                this.validator.validate(delete, RowKind.UPDATE_BEFORE);
                delete.setRowKind(RowKind.UPDATE_BEFORE);
                out.collect(delete);
                GenericRowData after = this.extractAfterRow(value, valueSchema);
                this.validator.validate(after, RowKind.UPDATE_AFTER);
                after.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(after);
            }
        } else {
            delete = this.extractAfterRow(value, valueSchema);
            this.validator.validate(delete, RowKind.INSERT);
            delete.setRowKind(RowKind.INSERT);
            out.collect(delete);
        }
    }

    private void initFieldListFull(SourceRecord record) {
        // Struct value = (Struct)record.value();
        Schema valueSchema = record.valueSchema();
        Schema afterSchema = valueSchema.field("after").schema();
        Schema beforeSchema = valueSchema.field("before").schema();
    }


    private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field("after").schema();
        Struct after = value.getStruct("after");
        return (GenericRowData)this.runtimeConverter.convert(after, afterSchema);
    }

    private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field("before").schema();
        Struct after = value.getStruct("before");
        return (GenericRowData)this.runtimeConverter.convert(after, afterSchema);
    }

    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    private MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter createConverter(LogicalType type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch(type.getTypeRoot()) {
        case NULL:
            return (dbzObj, schema) -> {
                return null;
            };
        case BOOLEAN:
            return this::convertToBoolean;
        case TINYINT:
            return (dbzObj, schema) -> {
                return Byte.parseByte(dbzObj.toString());
            };
        case SMALLINT:
            return (dbzObj, schema) -> {
                return Short.parseShort(dbzObj.toString());
            };
        case INTEGER:
        case INTERVAL_YEAR_MONTH:
            return this::convertToInt;
        case BIGINT:
        case INTERVAL_DAY_TIME:
            return this::convertToLong;
        case DATE:
            return this::convertToDate;
        case TIME_WITHOUT_TIME_ZONE:
            return this::convertToTime;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
            return this::convertToTimestamp;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return this::convertToLocalTimeZoneTimestamp;
        case FLOAT:
            return this::convertToFloat;
        case DOUBLE:
            return this::convertToDouble;
        case CHAR:
        case VARCHAR:
            return this::convertToString;
        case BINARY:
        case VARBINARY:
            return this::convertToBinary;
        case DECIMAL:
            return this.createDecimalConverter((DecimalType)type);
        case ROW:
            return this.createRowConverter((RowType)type);
        case ARRAY:
        case MAP:
        case MULTISET:
        case RAW:
        default:
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private boolean convertToBoolean(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Boolean) {
            return (Boolean)dbzObj;
        } else if (dbzObj instanceof Byte) {
            return (Byte)dbzObj == 1;
        } else if (dbzObj instanceof Short) {
            return (Short)dbzObj == 1;
        } else {
            return Boolean.parseBoolean(dbzObj.toString());
        }
    }

    private int convertToInt(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return (Integer)dbzObj;
        } else {
            return dbzObj instanceof Long ? ((Long)dbzObj).intValue() : Integer.parseInt(dbzObj.toString());
        }
    }

    private long convertToLong(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return (Long)dbzObj;
        } else {
            return dbzObj instanceof Long ? (Long)dbzObj : Long.parseLong(dbzObj.toString());
        }
    }

    private double convertToDouble(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return (Double)dbzObj;
        } else {
            return dbzObj instanceof Double ? (Double)dbzObj : Double.parseDouble(dbzObj.toString());
        }
    }

    private float convertToFloat(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return (Float)dbzObj;
        } else {
            return dbzObj instanceof Double ? ((Double)dbzObj).floatValue() : Float.parseFloat(dbzObj.toString());
        }
    }

    private int convertToDate(Object dbzObj, Schema schema) {
        return (int)TemporalConversions.toLocalDate(dbzObj).toEpochDay();
    }

    private int convertToTime(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            String var3 = schema.name();
            byte var4 = -1;
            switch(var3.hashCode()) {
            case -668140373:
                if (var3.equals("io.debezium.time.MicroTime")) {
                    var4 = 0;
                }
                break;
            case -218249369:
                if (var3.equals("io.debezium.time.NanoTime")) {
                    var4 = 1;
                }
            }

            switch(var4) {
            case 0:
                return (int)((Long)dbzObj / 1000L);
            case 1:
                return (int)((Long)dbzObj / 1000000L);
            }
        } else if (dbzObj instanceof Integer) {
            return (Integer)dbzObj;
        }

        return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            String var3 = schema.name();
            byte var4 = -1;
            switch(var3.hashCode()) {
            case -1830290952:
                if (var3.equals("io.debezium.time.MicroTimestamp")) {
                    var4 = 1;
                }
                break;
            case -1378581316:
                if (var3.equals("io.debezium.time.NanoTimestamp")) {
                    var4 = 2;
                }
                break;
            case -517856752:
                if (var3.equals("io.debezium.time.Timestamp")) {
                    var4 = 0;
                }
            }

            switch(var4) {
            case 0:
                return TimestampData.fromEpochMillis((Long)dbzObj);
            case 1:
                long micro = (Long)dbzObj;
                return TimestampData.fromEpochMillis(micro / 1000L, (int)(micro % 1000L * 1000L));
            case 2:
                long nano = (Long)dbzObj;
                return TimestampData.fromEpochMillis(nano / 1000000L, (int)(nano % 1000000L));
            }
        }

        LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, this.serverTimeZone);
        return TimestampData.fromLocalDateTime(localDateTime);
    }

    private TimestampData convertToLocalTimeZoneTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof String) {
            String str = (String)dbzObj;
            Instant instant = Instant.parse(str);
            return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, this.serverTimeZone));
        } else {
            throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value '" + dbzObj + "' of type " + dbzObj.getClass().getName());
        }
    }

    private StringData convertToString(Object dbzObj, Schema schema) {
        return StringData.fromString(dbzObj.toString());
    }

    private byte[] convertToBinary(Object dbzObj, Schema schema) {
        if (dbzObj instanceof byte[]) {
            return (byte[]) dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer)dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException("Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }

    private MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        return (dbzObj, schema) -> {
            BigDecimal bigDecimal;
            if (dbzObj instanceof byte[]) {
                bigDecimal = Decimal.toLogical(schema, dbzObj);
            } else if (dbzObj instanceof String) {
                bigDecimal = new BigDecimal((String)dbzObj);
            } else if (dbzObj instanceof Double) {
                bigDecimal = BigDecimal.valueOf((Double)dbzObj);
            } else if ("io.debezium.data.VariableScaleDecimal".equals(schema.name())) {
                SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct)dbzObj);
                bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
            } else {
                bigDecimal = new BigDecimal(dbzObj.toString());
            }

            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter createRowConverter(RowType rowType) {
        MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter[] fieldConverters =
                rowType
                .getFields()
                        .stream()
                        .map(RowField::getType)
                        .map(this::createConverter)
                        .toArray((x$0) -> {
            return new DeserializationRuntimeConverter[x$0];
        });
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        return (dbzObj, schema) -> {
            Struct struct = (Struct)dbzObj;
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);

            for(int i = 0; i < arity; ++i) {
                String fieldName = fieldNames[i];
                Object fieldValue = struct.get(fieldName);
                Schema fieldSchema = schema.field(fieldName).schema();
                Object convertedField = this.convertField(fieldConverters[i], fieldValue, fieldSchema);
                row.setField(i, convertedField);
            }

            return row;
        };
    }

    private Object convertField(MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter fieldConverter, Object fieldValue, Schema fieldSchema) throws Exception {
        return fieldValue == null ? null : fieldConverter.convert(fieldValue, fieldSchema);
    }

    private MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter wrapIntoNullableConverter(MysqlRowDataDebeziumDeserializeSchema.DeserializationRuntimeConverter converter) {
        return (dbzObj, schema) -> {
            return dbzObj == null ? null : converter.convert(dbzObj, schema);
        };
    }

    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object var1, Schema var2) throws Exception;
    }

    public interface ValueValidator extends Serializable {
        void validate(RowData var1, RowKind var2) throws Exception;
    }
}

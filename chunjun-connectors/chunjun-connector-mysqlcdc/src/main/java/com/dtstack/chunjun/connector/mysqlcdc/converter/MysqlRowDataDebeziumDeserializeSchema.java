package com.dtstack.chunjun.connector.mysqlcdc.converter;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class MysqlRowDataDebeziumDeserializeSchema implements DebeziumDeserializationSchema<RowData> {
    private String table;
    private String database;
    private static final long serialVersionUID = 1L;
    private TypeInformation<RowData> resultTypeInfo;
    private List<DataTypes.Field> dataTypes;
    private String[] columnNames;
    private DataType[] columnTypes;
    private Field[] columnIndex;
    private int arity;
    protected JdbcConf jdbcConf;
    private Boolean isInitialized = false;


    public MysqlRowDataDebeziumDeserializeSchema(String database, String table, List<DataTypes.Field> dataTypes, SyncConf syncConf) {

        this.arity = dataTypes.size();
        this.columnNames = new String[arity];
        this.columnIndex = new Field[this.arity];
        this.database = database;
        this.table = table;
        this.dataTypes = dataTypes;

//        JdbcUtil.getTableMetaData("",this.database,this.table,  null);
        for (int i = 0; i < arity; i++) {
            columnNames[i] = dataTypes.get(i).getName();
            columnTypes[i] = dataTypes.get(i).getDataType();
        }
    }


    public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
        if (!isInitialized) {
            initializeColumnIndex(record);
        }
        Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        GenericRowData delete;

        if (op != Operation.CREATE && op != Operation.READ) {
            if (op == Operation.DELETE) {
                delete = this.extractBeforeRow(value, valueSchema);
                delete.setRowKind(RowKind.DELETE);
                this.emit(record, delete, out);
            } else {
                delete = this.extractBeforeRow(value, valueSchema);
                delete.setRowKind(RowKind.UPDATE_BEFORE);
                this.emit(record, delete, out);
                GenericRowData after = this.extractAfterRow(value, valueSchema);
                after.setRowKind(RowKind.UPDATE_AFTER);
                this.emit(record, after, out);
            }
        } else {
            delete = this.extractAfterRow(value, valueSchema);
            delete.setRowKind(RowKind.INSERT);
            this.emit(record, delete, out);
        }


    }

    private void initializeColumnIndex(SourceRecord record) {
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        Schema afterSchema = valueSchema.field("after").schema();
        Schema beforeSchema = valueSchema.field("before").schema();
        Schema sourceSource = afterSchema != null ? afterSchema : beforeSchema;
        String[] columnNamesFull = (String[]) sourceSource.fields().stream().map(Field::name).toArray();
        for (int i = 0; i < this.columnNames.length; i++) {
            for (int j = 0; j < columnNamesFull.length; j++) {
                String columnNameKafka=columnNamesFull[j];
                if (columnNames[i].equalsIgnoreCase( columnNameKafka)){
                    columnIndex[i]=new Field(columnNameKafka,j,null);
                }
            }
        }
    }

    private void emit(SourceRecord inRecord, RowData physicalRow, Collector<RowData> collector) {
        Struct value = (Struct) inRecord.value();
        for(int i=0;i<this.columnIndex.length;i++){
            Object v = value.get(this.columnIndex[i]);

        }
        collector.collect(physicalRow);

    }

    private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field("after").schema();
        Struct after = value.getStruct("after");

        return null;
    }

    private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field("before").schema();
        Struct before = value.getStruct("before");
        return null;
    }


    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }


}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.mysqlcdc.converter;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;

import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import com.dtstack.chunjun.connector.mysqlcdc.enums.UpdateType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 生成内部的数据格式（数据类型AbstractBaseColumn）
 * 同时符合：DebeziumDeserializationSchema接口
 *
 * @program: ChunJun
 * @author: wuren
 * @create: 2021/04/14
 */
public class MysqlCdcColumnTypeConverter extends AbstractCDCRowConverter<SourceRecord, LogicalType> implements DebeziumDeserializationSchema<RowData> {

    public String database;
    public String schema;
    public String table;
    public List<ImmutablePair<String, IDeserializationConverter>> runtimeConverters = new ArrayList<>();
    public int arity;
    private static String currentBinlogFileName;
    private static BigDecimal currentBinlogFilePos;
    private final TypeInformation<RowData> resultTypeInfo;
    private List<ImmutablePair<String, LogicalType>> logicalTypes;

    public MysqlCdcColumnTypeConverter(TypeInformation<RowData> resultTypeInfo) {
        this.resultTypeInfo = resultTypeInfo;
        logicalTypes = new ArrayList<>();
        ((RowType) ((InternalTypeInfo) resultTypeInfo).toLogicalType()).getFields().stream().forEach(field -> {
            logicalTypes.add(new ImmutablePair<>(field.getName(), field.getType()));
        });
        arity=logicalTypes.size();
        logicalTypes.stream().forEach(pair -> {
            runtimeConverters.add(new ImmutablePair<>(pair.getLeft(), createInternalConverter(pair.getRight())));
        });

    }

//    public MysqlCdcColumnTypeConverter(String database, String schema, String table) {
//        this.database = database;
//        this.schema = schema;
//        this.table = table;
//    }

    // TODO: lisai 自定义转换器
    @Override
    public LinkedList<RowData> toInternal(SourceRecord record) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
//        // 获取到原数据信息
//        this.database = value.getStruct("source").get("db").toString();
//        this.schema = value.getStruct("source").get("schema").toString();
//        this.table = value.getStruct("source").get("table").toString();
//        this.currentBinlogFileName = value.getStruct("source").get("file").toString();
//        this.currentBinlogFilePos = new BigDecimal(value.getStruct("source").get("pos").toString());
        RowData delete;
        RowData after;
        LinkedList<RowData> res = new LinkedList<>();
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            // 如果是新增数据：create
            if (op == Envelope.Operation.DELETE) {
                delete = this.extractBeforeRow(value, valueSchema);
//                this.validator.validate(delete, RowKind.DELETE);
                delete.setRowKind(RowKind.DELETE);
                res.add(delete);
            } else {
                delete = this.extractBeforeRow(value, valueSchema);
//                this.validator.validate(delete, RowKind.UPDATE_BEFORE);
                delete.setRowKind(RowKind.UPDATE_BEFORE);
                // TODO: lisai update 前的数据是否需要保留？
                res.add(delete);
                after = this.extractAfterRow(value, valueSchema);
//                this.validator.validate(after, RowKind.UPDATE_AFTER);
                after.setRowKind(RowKind.UPDATE_AFTER);
                res.add(after);
            }
        } else {
            delete = this.extractAfterRow(value, valueSchema);
//            this.validator.validate(delete, RowKind.INSERT);
            delete.setRowKind(RowKind.INSERT);
            res.add(delete);
        }
        return res;
    }

    private RowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
//        Schema afterSchema = valueSchema.field(UpdateType.AFTER.getName()).schema();
        Struct after = value.getStruct(UpdateType.AFTER.getName());
        return genRowData(UpdateType.AFTER, after);
    }

    private RowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
//        Schema beforeSchema = valueSchema.field(UpdateType.BEFORE.getName()).schema();
        Struct before = value.getStruct(UpdateType.BEFORE.getName());
        return genRowData(UpdateType.BEFORE, before);
    }

    private RowData genRowData(UpdateType updateType, Struct struct) throws Exception {
        String columnName = "";
        Object columnValue = "";
        ColumnRowData rowData = new ColumnRowData(arity);
        for (int i = 0; i < arity; i++) {
            try {
                columnName = runtimeConverters.get(i).getLeft();
                columnValue = struct.get(columnName).toString();
                rowData.addField((AbstractBaseColumn) runtimeConverters.get(i).getRight().deserialize(columnValue));
            } catch (Exception e) {
                LOG.info("Converter field {} failed with {} value of {}", columnName, updateType.getName(), columnValue.toString());
                throw new Exception(e.getMessage());
            }
        }
        return rowData;
    }

    /**
     * 初始化列信息和转换器列表
     */
    public void initColumnInfoAndConverter() {
//        logicalTypes.stream().forEach(pair -> {
//            runtimeConverters.add(new ImmutablePair<>(pair.getLeft(), createInternalConverter(pair.getRight())));
//        });
//        for (Field field : schema.fields()) {
//            String columnName = field.name();
//            Integer columnIndex = field.index();
//            String columnType = field.schema().type().getName();
//            if ("before".equalsIgnoreCase(type)) {
//                schemaBefore.add(new ImmutableTriple<>(columnName, columnIndex, columnType));
//            } else if ("after".equalsIgnoreCase(type)) {
//                schemaAfter.add(new ImmutableTriple<>(columnName, columnIndex, columnType));
//            }
//        }
        // 设置数据捕捉标志符，避免每次都捕捉性能差。并初始化converter
//        if (schemaBefore.size() > 0) {
//            schemaBefore.stream().forEach(t -> {
//                convertersBefore.add(new ImmutablePair<>(t.getLeft(), createInternalConverter(t.getRight())));
//            });
//            schemaBeforeFlag = true;
//        } else if (schemaAfter.size() > 0) {
//            schemaAfter.stream().forEach(t -> {
//                convertersBefore.add(new ImmutablePair<>(t.getLeft(), createInternalConverter(t.getRight())));
//            });
//            schemaAfterFlag = true;
//        }

    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        // TODO: lisai 包org.apache.kafka.connect.data中的Schema含有struct的类型汇总,其没有日期等类型和原数据库不符，不能使用。
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
            case DECIMAL:
                return val -> new BigDecimalColumn((String) val);
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val -> new SqlDateColumn(Date.valueOf(String.valueOf(val)));
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn(Time.valueOf(String.valueOf(val)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn(Timestamp.valueOf(String.valueOf(val)), 6);
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn(((String) val).getBytes(StandardCharsets.UTF_8));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * DebeziumDeserializationSchema 实现
     *
     * @param sourceRecord
     * @param collector
     *
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<RowData> collector) throws Exception {
        Struct value = (Struct) sourceRecord.value();
//        Schema valueSchema = sourceRecord.valueSchema();

        try {
            // 获取到原数据信息
            this.database = value.getStruct("source").get("db").toString();
            this.schema = value.getStruct("source").get("schema").toString();
            this.table = value.getStruct("source").get("table").toString();
            currentBinlogFileName = value.getStruct("source").get("file").toString();
            currentBinlogFilePos = new BigDecimal(value.getStruct("source").get("pos").toString());
        } catch (Exception e) {
//            LOG.info("SourceRecord get meta error: {}", e.getMessage());
        }

        LinkedList<RowData> rowData = this.toInternal(sourceRecord);
        rowData.forEach(collector::collect);
    }

    /**
     * DebeziumDeserializationSchema 实现
     *
     * @return
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }
}

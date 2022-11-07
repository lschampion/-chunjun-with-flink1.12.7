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
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import com.dtstack.chunjun.connector.mysqlcdc.enums.UpdateType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * 生成内部的数据格式（数据类型AbstractBaseColumn）
 * 同时符合：DebeziumDeserializationSchema接口
 * @program: ChunJun
 * @author: wuren
 * @create: 2021/04/14
 */
public class MysqlCdcColumnTypeConverter extends AbstractCDCRowConverter<SourceRecord, String> implements DebeziumDeserializationSchema<RowData> {

    public String database;
    public String schema;
    public String table;
    public
    List<ImmutableTriple<String, Integer, String>> schemaBefore = new ArrayList<>();
    List<ImmutableTriple<String, Integer, String>> schemaAfter = new ArrayList<>();
    List<ImmutablePair<String, IDeserializationConverter>> convertersBefore = new ArrayList<>();
    List<ImmutablePair<String, IDeserializationConverter>> convertersAfter = new ArrayList<>();
    private boolean schemaBeforeFlag = false;
    private boolean schemaAfterFlag = false;
    private static String currentBinlogFileName;
    private static BigDecimal currentBinlogFilePos;
    private final TypeInformation<RowData> resultTypeInfo;

    public MysqlCdcColumnTypeConverter(TypeInformation<RowData> resultTypeInfo) {
        this.resultTypeInfo = resultTypeInfo;
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
        // init after
        Schema afterSchema = valueSchema.field(UpdateType.AFTER.getName()).schema();
        Struct after = value.getStruct(UpdateType.AFTER.getName());
        if (!schemaAfterFlag) {
            initColumnInfoAndConverter(afterSchema, UpdateType.AFTER.getName());
        }
        return genRowData(UpdateType.AFTER, after);
    }

    private RowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field(UpdateType.BEFORE.getName()).schema();
        Struct before = value.getStruct(UpdateType.BEFORE.getName());
        if (!schemaBeforeFlag) {
            initColumnInfoAndConverter(beforeSchema, UpdateType.BEFORE.getName());
        }
        return genRowData(UpdateType.BEFORE, before);
    }

    private RowData genRowData(UpdateType updateType, Struct struct) throws Exception {
        List<ImmutablePair<String, IDeserializationConverter>> converters;
        int arity = 0;
        if (UpdateType.AFTER == updateType) {
            converters = this.convertersAfter;
            arity = this.convertersAfter.size();
        } else
//            if (UpdateType.BEFORE == updateType)
        {
            converters = this.convertersBefore;
            arity = this.convertersBefore.size();
        }
        String columnName = "";
        Object columnValue = "";
        ColumnRowData rowData = new ColumnRowData(arity);
        for (int i = 0; i < arity; i++) {
            try {
                columnName = converters.get(i).getLeft();
                columnValue = struct.get(columnName).toString();
                rowData.addField((AbstractBaseColumn) converters.get(i).getRight().deserialize(columnValue));
            } catch (Exception e) {
                LOG.info("Converter field {} failed with {} value of {}", columnName, updateType.getName(), columnValue.toString());
                throw new Exception(e.getMessage());
            }
        }
        return rowData;

    }

    /**
     * 初始化列信息和转换器列表
     *
     * @param schema
     * @param type after or before
     */
    public void initColumnInfoAndConverter(Schema schema, String type) {
        for (Field field : schema.fields()) {
            String columnName = field.name();
            Integer columnIndex = field.index();
            String columnType = field.schema().type().getName();
            if ("before".equalsIgnoreCase(type)) {
                schemaBefore.add(new ImmutableTriple<>(columnName, columnIndex, columnType));
            } else if ("after".equalsIgnoreCase(type)) {
                schemaAfter.add(new ImmutableTriple<>(columnName, columnIndex, columnType));
            }
        }
        // 设置数据捕捉标志符，避免每次都捕捉性能差。并初始化converter
        if (schemaBefore.size() > 0) {
            schemaBefore.stream().forEach(t -> {
                convertersBefore.add(new ImmutablePair<>(t.getLeft(), createInternalConverter(t.getRight())));
            });
            schemaBeforeFlag = true;
        } else if (schemaAfter.size() > 0) {
            schemaAfter.stream().forEach(t -> {
                convertersBefore.add(new ImmutablePair<>(t.getLeft(), createInternalConverter(t.getRight())));
            });
            schemaAfterFlag = true;
        }
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        java.lang.String substring = type;
        // 为了支持无符号类型  如 int unsigned
        if (StringUtils.contains(substring, ConstantValue.DATA_TYPE_UNSIGNED)) {
            substring = substring.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED, "").trim();
        }
        if (StringUtils.contains(substring, ConstantValue.DATA_TYPE_UNSIGNED_LOWER)) {
            substring = substring.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED_LOWER, "").trim();
        }
        int index = substring.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
        if (index > 0) {
            substring = substring.substring(0, index);
        }
        switch (substring.toUpperCase(Locale.ENGLISH)) {
            case "BIT":
                return (IDeserializationConverter<java.lang.String, AbstractBaseColumn>)
                        val -> new BooleanColumn(Boolean.parseBoolean(val));
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INT24":
            case "INTEGER":
            case "FLOAT":
            case "DOUBLE":
            case "REAL":
            case "LONG":
            case "BIGINT":
            case "DECIMAL":
            case "NUMERIC":
                return (IDeserializationConverter<java.lang.String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "ENUM":
            case "SET":
            case "JSON":
                return (IDeserializationConverter<java.lang.String, AbstractBaseColumn>) StringColumn::new;
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
            case "DATETIME":
            case "YEAR":
                return (IDeserializationConverter<java.lang.String, AbstractBaseColumn>)
                        val -> new TimestampColumn(DateUtil.getTimestampFromStr(val));
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "GEOMETRY":
            case "BINARY":
            case "VARBINARY":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BytesColumn(val.getBytes(StandardCharsets.UTF_8));
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
        // 获取到原数据信息
        this.database = value.getStruct("source").get("db").toString();
        this.schema = value.getStruct("source").get("schema").toString();
        this.table = value.getStruct("source").get("table").toString();
        currentBinlogFileName = value.getStruct("source").get("file").toString();
        currentBinlogFilePos = new BigDecimal(value.getStruct("source").get("pos").toString());

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

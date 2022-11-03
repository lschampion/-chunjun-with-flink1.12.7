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

package com.dtstack.chunjun.connector.mysqlcdc.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.conf.CdcConf;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MySqlReadableMetadata;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlCdcRawTypeConverter;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlMetadataConverter;
import com.dtstack.chunjun.connector.mysqlcdc.entities.Student;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import com.dtstack.chunjun.util.TableUtil;


import com.google.common.collect.Lists;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import com.ververica.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class MysqlcdcSourceFactory extends SourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlcdcSourceFactory.class);

    protected CdcConf cdcConf;

    public MysqlcdcSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SourceConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        cdcConf = gson.fromJson(gson.toJson(syncConf.getReader().getParameter()), getConfClass());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlCdcRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        List<DataTypes.Field> dataTypes =
                new ArrayList<>(syncConf.getReader().getFieldList().size());
        syncConf.getReader()
                .getFieldList()
                .forEach(
                        fieldConf -> {
                            dataTypes.add(
                                    DataTypes.FIELD(
                                            fieldConf.getName(),
                                            getRawTypeConverter().apply(fieldConf.getType())));
                        });
        final DataType dataType = DataTypes.ROW(dataTypes.toArray(new DataTypes.Field[0]));
        RowType rowType = TableUtil.createRowType(fieldList, getRawTypeConverter());

        MetadataConverter[] metadataConverters = new MetadataConverter[3];
        metadataConverters[0] = MySqlReadableMetadata.DATABASE_NAME.getConverter();
        metadataConverters[1] = MySqlReadableMetadata.TABLE_NAME.getConverter();
        metadataConverters[2] = MySqlReadableMetadata.OP_TS.getConverter();
        RowDataDebeziumDeserializeSchema rowDataDebeziumDeserializeSchema = RowDataDebeziumDeserializeSchema.newBuilder()
                .setMetadataConverters(metadataConverters)
                .setServerTimeZone(ZoneOffset.UTC)
                .setValueValidator(new DemoValueValidator())
                .setUserDefinedConverterFactory(MySqlDeserializationConverterFactory.instance())
                .setResultTypeInfo(getTypeInformation())
                .setPhysicalRowType(rowType)
                .build();
//        MySqlSource<RowData> mySqlSource = MySqlSource.<RowData>builder()
//                .hostname(cdcConf.getHost())
//                .port(cdcConf.getPort())
//                .databaseList(cdcConf.getDatabaseList().toArray(new String[0])) // set captured database
//                .tableList(
//                        cdcConf.getTableList()
//                                .toArray(new String[cdcConf.getDatabaseList().size()])) // set captured table
//                .username(cdcConf.getUsername())
//                .password(cdcConf.getPassword())
////                .serverId(String.valueOf(cdcConf.getServerId()))
//                .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
//                .startupOptions(StartupOptions.latest())
//                .build();


        MySqlSource<RowData> mySqlSource = MySqlSource.<RowData>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList(Lists.newArrayList("test.student").toArray(new String[0])) // set captured table
                .username("root")
                .password("123456")
                .deserializer(rowDataDebeziumDeserializeSchema)
                .startupOptions(StartupOptions.latest())
                .build();
//
//        MySqlSource<String> mySqlSourceJson = MySqlSource.<String>builder()
//                .hostname("localhost")
//                .port(3306)
//                .databaseList("test") // set captured database
//                .tableList(Lists.newArrayList("test.student").toArray(new String[0])) // set captured table
//                .username("root")
//                .password("123456")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.latest())
//                .build();


        DataStreamSource<RowData> mysqlCdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlCdcSource");
//        DataStreamSource<String> mysqlCdcSourceJson = env.fromSource(mySqlSourceJson, WatermarkStrategy.noWatermarks(), "MysqlCdcSource");
        mysqlCdcSource.print().setParallelism(1);


        ArrayList<RowData> columnRowData = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            ArrayList<AbstractBaseColumn> col = Lists.newArrayList(new BigDecimalColumn(i), new StringColumn("name_" + i), new StringColumn("男"));
            columnRowData.add((RowData) (new ColumnRowData(RowKind.INSERT, 5).setColumnList(col)));
        }

//        DataStreamSource<RowData> mysqlCdcSource1 = env.fromCollection(columnRowData);
        return mysqlCdcSource;
    }

    private DebeziumDeserializationSchema<RowData> buildRowDataDebeziumDeserializeSchema(
            DataType dataType) {
        List<FieldConf> fieldList = syncConf.getReader().getFieldList();
        RowType rowType = TableUtil.createRowType(fieldList, getRawTypeConverter());
        RowDataDebeziumDeserializeSchema schema = new RowDataDebeziumDeserializeSchema.Builder()
                .setPhysicalRowType((RowType) dataType.getLogicalType())
                .setMetadataConverters(Lists.newArrayList(new MysqlMetadataConverter()).toArray(new MysqlMetadataConverter[0]))
                .setResultTypeInfo(getTypeInformation())
                .setServerTimeZone(ZoneOffset.UTC)
                .setValueValidator(new DemoValueValidator())
                .build();
        return schema;
    }

    protected Class<? extends CdcConf> getConfClass() {
        return CdcConf.class;
    }

    public static final class DemoValueValidator
            implements RowDataDebeziumDeserializeSchema.ValueValidator {

        @Override
        public void validate(RowData rowData, RowKind rowKind) {
            // do nothing
        }
    }
}

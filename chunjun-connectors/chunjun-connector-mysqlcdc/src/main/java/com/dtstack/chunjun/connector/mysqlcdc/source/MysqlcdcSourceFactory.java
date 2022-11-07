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

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.conf.CdcConf;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlCdcRawTypeConverter;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlRowDataDebeziumDeserializeSchema;
import com.dtstack.chunjun.connector.mysqlcdc.metainfo.CdcMetaInfo;
import com.dtstack.chunjun.converter.RawTypeConverter;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.enums.EDatabaseType;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlcdcSourceFactory extends SourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlcdcSourceFactory.class);
    protected CdcConf cdcConf;
    protected List<CdcMetaInfo> cdcMetaInfos;
    protected boolean isSingleTableWithSelect;

    public MysqlcdcSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        List<FieldConf> fieldList = syncConf.getReader().getFieldList();
        if (fieldList != null && fieldList.size() > 0) {
            if (fieldList.size() == 1 && "*".equals(fieldList.get(0).getName())) {
                isSingleTableWithSelect = false;
            } else {
                isSingleTableWithSelect = false;
            }
        } else {
            isSingleTableWithSelect = true;
        }
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SourceConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        cdcConf = gson.fromJson(gson.toJson(syncConf.getReader().getParameter()), getConfClass());
        initTableMetaInfos(cdcConf, EDatabaseType.MySQL);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlCdcRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        List<DataTypes.Field> dataTypes = new ArrayList<>(syncConf.getReader().getFieldList().size());
//        TableUtil.createRowType(Arrays.asList(columnNameMap), Arrays.asList(columnTypeMap), getRawTypeConverter());

        // TODO: lisai 判断是否是"*",else 中是原始内容
        if (syncConf.getReader().getFieldList().size() == 1 && "*".equals(syncConf.getReader().getFieldList().get(0).getName())) {
            LOG.info("Cdc Source use full columns for reason of \"*\")");

            CdcMetaInfo cdcMetaInfo = this.cdcMetaInfos.get(0);
            cdcMetaInfo.getColumnMetaFull()
                    .forEach(col -> {
                        dataTypes.add(DataTypes.FIELD(col.getLeft(), col.getRight()));
                    });
        } else {
            syncConf.getReader()
                    .getFieldList()
                    .forEach(
                            fieldConf -> {
                                dataTypes.add(
                                        DataTypes.FIELD(
                                                fieldConf.getName(),
                                                getRawTypeConverter().apply(fieldConf.getType())));
                            });
        }

        final DataType dataType = DataTypes.ROW(dataTypes.toArray(new DataTypes.Field[0]));

//        RowType rowType = TableUtil.createRowType(fieldList, getRawTypeConverter());
//        MetadataConverter[] metadataConverters = new MetadataConverter[3];
//        metadataConverters[0] = MySqlReadableMetadata.DATABASE_NAME.getConverter();
//        metadataConverters[1] = MySqlReadableMetadata.TABLE_NAME.getConverter();
//        metadataConverters[2] = MySqlReadableMetadata.OP_TS.getConverter();
//        RowDataDebeziumDeserializeSchema rowDataDebeziumDeserializeSchema = RowDataDebeziumDeserializeSchema.newBuilder()
//                .setMetadataConverters(metadataConverters)
//                .setServerTimeZone(ZoneOffset.UTC)
//                .setValueValidator(new DemoValueValidator())
//                .setUserDefinedConverterFactory(MySqlDeserializationConverterFactory.instance())
//                .setResultTypeInfo(getTypeInformation())
//                .setPhysicalRowType(rowType)
//                .build();

//      新版本可以使用：MySqlSource<RowData> mySqlSource = MySqlSource.<RowData>builder()
        DebeziumSourceFunction<RowData> mySqlSource =
                new MySQLSource.Builder<RowData>()
                        .hostname(cdcConf.getHost())
                        .port(cdcConf.getPort())
                        .databaseList(Lists.newArrayList(cdcConf.getDatabase()).toArray(new String[0])) // set captured database
                        .tableList(
                                cdcConf.getTableList()
                                        .toArray(new String[cdcConf.getSchemaList().size()])) // set captured table
                        .username(cdcConf.getUsername())
                        .password(cdcConf.getPassword())
                        .serverId(cdcConf.getServerId())
                        .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
//                        .startupOptions(StartupOptions.initial())
                        .build();

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


        DataStreamSource<RowData> mysqlCdcSource = env.addSource(mySqlSource, "MysqlCdcSource", getTypeInformation());
        mysqlCdcSource.print().setParallelism(1);
        return mysqlCdcSource;
    }


    private void initTableMetaInfos(CdcConf cdcConf, EDatabaseType databaseType) {
        String host = cdcConf.getHost();
        int port = cdcConf.getPort();
        String database = cdcConf.getDatabase();
        String username = cdcConf.getUsername();
        String password = cdcConf.getPassword();
        // List<String> schemaList = cdcConf.getSchemaList();
        List<CdcMetaInfo> infos = cdcConf.getTableList().stream().map(tableOrigin -> {
            String[] fullName = tableOrigin.split("\\.");
            String databaseOrSchemaName = fullName.length == 2 ? fullName[0] : "";
            String tableName = fullName.length == 2 ? fullName[1] : tableOrigin;
            // TODO: initTableMetaInfos 其他数据库也需要考虑
            String schema = "";
            String table = "";
            Pair<String, String> schemaTable = new MutablePair<>();
            switch (databaseType) {
                case MySQL:
                    schema = "";
                    table = tableName;
                    break;
                case PostgreSQL:
                    schema = databaseOrSchemaName;
                    table = tableName;
                    break;
                default:
            }
            CdcMetaInfo info = new CdcMetaInfo(databaseType, host, port, username, password, database, schema, table, isSingleTableWithSelect);
            return info;
        }).collect(Collectors.toList());
        this.cdcMetaInfos = infos;
    }

    /**
     * 测试数据生成器
     *
     * @return
     */
    public ArrayList<RowData> mockSourceList() {
        ArrayList<RowData> columnRowData = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ArrayList<AbstractBaseColumn> col = Lists.newArrayList(new BigDecimalColumn(i), new StringColumn("name_" + i), new StringColumn("男"));
            columnRowData.add(new ColumnRowData(RowKind.INSERT, 5).setColumnList(col));
        }
        //  使用方法： DataStreamSource<RowData> mysqlCdcSource = env.fromCollection(columnRowData);
        return columnRowData;
    }

    private DebeziumDeserializationSchema<RowData> buildRowDataDebeziumDeserializeSchema(DataType dataType) {
        MysqlRowDataDebeziumDeserializeSchema schema = new MysqlRowDataDebeziumDeserializeSchema(
                (RowType) dataType.getLogicalType(),
                getTypeInformation(),
                new DemoValueValidator(),
                ZoneOffset.UTC);
        return schema;
    }

    protected Class<? extends CdcConf> getConfClass() {
        return CdcConf.class;
    }
    public static final class DemoValueValidator
            implements MysqlRowDataDebeziumDeserializeSchema.ValueValidator {
        @Override
        public void validate(RowData rowData, RowKind rowKind) {
            // do nothing
        }
    }


}

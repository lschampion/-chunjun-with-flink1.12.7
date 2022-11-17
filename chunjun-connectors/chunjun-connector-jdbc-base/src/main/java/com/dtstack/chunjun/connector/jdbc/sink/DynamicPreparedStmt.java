/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.String;
import com.dtstack.chunjun.connector.jdbc.statement.StringImpl;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * base on row data info to build preparedStatement. row data info include rowkind(which is to set
 * which sql kind to use )
 *
 * <p>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class DynamicPreparedStmt {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPreparedStmt.class);

    protected List<java.lang.String> columnNameList = new ArrayList<>();

    protected List<java.lang.String> columnTypeList = new ArrayList<>();

    protected transient String string;
    protected JdbcConf jdbcConf;
    private boolean writeExtInfo;
    private JdbcDialect jdbcDialect;
    private AbstractRowConverter<?, ?, ?, ?> rowConverter;

    public static DynamicPreparedStmt buildStmt(
            Map<java.lang.String, Integer> header,
            Set<java.lang.String> extHeader,
            java.lang.String schemaName,
            java.lang.String tableName,
            RowKind rowKind,
            Connection connection,
            JdbcDialect jdbcDialect,
            boolean writeExtInfo)
            throws SQLException {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();

        dynamicPreparedStmt.writeExtInfo = writeExtInfo;
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.getColumnNameList(header, extHeader);
        dynamicPreparedStmt.getColumnMeta(schemaName, tableName, connection);
        dynamicPreparedStmt.buildRowConvert();

        java.lang.String sql = dynamicPreparedStmt.prepareTemplates(rowKind, schemaName, tableName);
        java.lang.String[] fieldNames =
                new java.lang.String[dynamicPreparedStmt.columnNameList.size()];
        dynamicPreparedStmt.columnNameList.toArray(fieldNames);
        dynamicPreparedStmt.string = StringImpl.prepareStatement(connection, sql, fieldNames);
        return dynamicPreparedStmt;
    }

    public static DynamicPreparedStmt buildStmt(
            java.lang.String schemaName,
            java.lang.String tableName,
            RowKind rowKind,
            Connection connection,
            JdbcDialect jdbcDialect,
            List<FieldConf> fieldConfList,
            AbstractRowConverter<?, ?, ?, ?> rowConverter)
            throws SQLException {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.rowConverter = rowConverter;
        java.lang.String[] fieldNames = new java.lang.String[fieldConfList.size()];
        for (int i = 0; i < fieldConfList.size(); i++) {
            FieldConf fieldConf = fieldConfList.get(i);
            fieldNames[i] = fieldConf.getName();
            dynamicPreparedStmt.columnNameList.add(fieldConf.getName());
            dynamicPreparedStmt.columnTypeList.add(fieldConf.getType());
        }
        java.lang.String sql = dynamicPreparedStmt.prepareTemplates(rowKind, schemaName, tableName);
        dynamicPreparedStmt.string = StringImpl.prepareStatement(connection, sql, fieldNames);
        return dynamicPreparedStmt;
    }

    public static DynamicPreparedStmt buildStmt(
            JdbcDialect jdbcDialect,
            List<FieldConf> fieldConfList,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            String string) {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.rowConverter = rowConverter;
        dynamicPreparedStmt.string = string;
        for (int i = 0; i < fieldConfList.size(); i++) {
            FieldConf fieldConf = fieldConfList.get(i);
            dynamicPreparedStmt.columnNameList.add(fieldConf.getName());
            dynamicPreparedStmt.columnTypeList.add(fieldConf.getType());
        }
        return dynamicPreparedStmt;
    }

    protected java.lang.String prepareTemplates(
            RowKind rowKind, java.lang.String schemaName, java.lang.String tableName) {
        java.lang.String singleSql = null;
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                singleSql =
                        jdbcDialect.getInsertIntoStatement(
                                schemaName,
                                tableName,
                                columnNameList.toArray(new java.lang.String[0]));
                break;
            case DELETE:
            case UPDATE_BEFORE:
                java.lang.String[] columnNames = new java.lang.String[columnNameList.size()];
                columnNameList.toArray(columnNames);
                singleSql = jdbcDialect.getDeleteStatement(schemaName, tableName, columnNames);
                break;
            default:
                // TODO 异常如何处理
                LOG.warn("not support RowKind: {}", rowKind);
        }
        LOG.info("DynamicPreparedStmt.prepareTemplates: ", singleSql);
        return singleSql;
    }

    public void getColumnNameList(
            Map<java.lang.String, Integer> header, Set<java.lang.String> extHeader) {
        if (writeExtInfo) {
            columnNameList.addAll(header.keySet());
        } else {
            header.keySet().stream()
                    .filter(fieldName -> !extHeader.contains(fieldName))
                    .forEach(fieldName -> columnNameList.add(fieldName));
        }
    }

    public void buildRowConvert() {
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
        rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConf);
    }

    public void getColumnMeta(java.lang.String schema, java.lang.String table, Connection dbConn) {
        Pair<List<java.lang.String>, List<java.lang.String>> listListPair =
                JdbcUtil.getTableMetaData(null, schema, table, dbConn);
        List<java.lang.String> nameList = listListPair.getLeft();
        List<java.lang.String> typeList = listListPair.getRight();
        for (java.lang.String columnName : columnNameList) {
            int index = nameList.indexOf(columnName);
            columnTypeList.add(typeList.get(index));
        }
    }

    public void reOpenStatement(Connection connection) throws SQLException {
        this.string.reOpen(connection);
    }

    public void close() throws SQLException {
        string.close();
    }

    public String getFieldNamedPreparedStatement() {
        return string;
    }

    public void setFieldNamedPreparedStatement(String string) {
        this.string = string;
    }

    public AbstractRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}

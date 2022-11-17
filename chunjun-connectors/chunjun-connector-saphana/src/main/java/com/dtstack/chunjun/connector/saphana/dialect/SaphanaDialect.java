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

package com.dtstack.chunjun.connector.saphana.dialect;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.String;
import com.dtstack.chunjun.connector.saphana.converter.SaphanaColumnConverter;
import com.dtstack.chunjun.connector.saphana.converter.SaphanaRawTypeConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.enums.EDatabaseType;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class SaphanaDialect implements JdbcDialect {

    @Override
    public java.lang.String dialectName() {
        return EDatabaseType.SapHana.name();
    }

    @Override
    public boolean canHandle(java.lang.String url) {
        return url.startsWith("jdbc:sap:");
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return SaphanaRawTypeConverter::apply;
    }

    @Override
    public Optional<java.lang.String> defaultDriverName() {
        return Optional.of("com.sap.db.jdbc.Driver");
    }

    @Override
    public Optional<java.lang.String> getUpsertStatement(
            java.lang.String schema,
            java.lang.String tableName,
            java.lang.String[] fieldNames,
            java.lang.String[] uniqueKeyFields,
            boolean allReplace) {
        tableName = buildTableInfoWithSchema(schema, tableName);
        StringBuilder mergeIntoSql = new StringBuilder(64);
        mergeIntoSql
                .append("MERGE INTO ")
                .append(tableName)
                .append(" T1 USING (")
                .append(buildDualQueryStatement(fieldNames))
                .append(") T2 ON (")
                .append(buildEqualConditions(uniqueKeyFields))
                .append(") ");

        java.lang.String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql
                .append(" WHEN NOT MATCHED THEN ")
                .append("INSERT (")
                .append(
                        Arrays.stream(fieldNames)
                                .map(this::quoteIdentifier)
                                .collect(Collectors.joining(", ")))
                .append(") VALUES (")
                .append(
                        Arrays.stream(fieldNames)
                                .map(col -> "T2." + quoteIdentifier(col))
                                .collect(Collectors.joining(", ")))
                .append(")");

        return Optional.of(mergeIntoSql.toString());
    }

    @Override
    public java.lang.String quoteIdentifier(java.lang.String identifier) {
        return identifier;
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, String, LogicalType> getColumnConverter(
            RowType rowType, ChunJunCommonConf commonConf) {
        return new SaphanaColumnConverter(rowType, commonConf);
    }

    @Override
    public java.lang.String getRowNumColumn(java.lang.String orderBy) {
        return "rownum as CHUNJUN_ROWNUM";
    }

    /** build select sql , such as (SELECT ? "A",? "B" FROM DUAL) */
    public java.lang.String buildDualQueryStatement(java.lang.String[] column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        java.lang.String collect =
                Arrays.stream(column)
                        .map(col -> ":" + col + " " + quoteIdentifier(col))
                        .collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM \"SYS\".\"DUMMY\"");
        return sb.toString();
    }

    /** build sql part e.g: T1.`A` = T2.`A`, T1.`B` = T2.`B` */
    private java.lang.String buildEqualConditions(java.lang.String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields)
                .map(col -> "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));
    }

    /** build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A") */
    private java.lang.String buildUpdateConnection(
            java.lang.String[] fieldNames, java.lang.String[] uniqueKeyFields, boolean allReplace) {
        List<java.lang.String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return Arrays.stream(fieldNames)
                .filter(col -> !uniqueKeyList.contains(col))
                .map(col -> buildConnectString(allReplace, col))
                .collect(Collectors.joining(","));
    }

    /**
     * Depending on parameter [allReplace] build different sql part. e.g T1."A"=T2."A" or
     * T1."A"=nvl(T2."A",T1."A")
     */
    private java.lang.String buildConnectString(boolean allReplace, java.lang.String col) {
        return allReplace
                ? "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col)
                : "T1."
                        + quoteIdentifier(col)
                        + " =IFNULL(T2."
                        + quoteIdentifier(col)
                        + ",T1."
                        + quoteIdentifier(col)
                        + ")";
    }
}

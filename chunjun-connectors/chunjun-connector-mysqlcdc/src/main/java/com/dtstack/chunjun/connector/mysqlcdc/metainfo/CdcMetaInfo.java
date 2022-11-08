package com.dtstack.chunjun.connector.mysqlcdc.metainfo;


import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlCdcRawTypeConverter;
import com.dtstack.chunjun.enums.EDatabaseType;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CdcMetaInfo {
    private static final Logger LOG = LoggerFactory.getLogger(CdcMetaInfo.class);

    public EDatabaseType databaseType;
    public String host;
    public int port;
    public String username;
    public String password;
    public String database;
    public String schema;
    public String table;
    public String jdbcUrl;
    public String[] fieldArr;

    public List<Triple<String, String, DataType>> columnMetaFull;
    public List<Triple<String, String, DataType>> columnMetaSelected;
    public boolean isSingleTableWithSelect = false;
    public RowType rowType;
    protected String defaultDriverName;
    protected String defaultJdbcUrl;

    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }

    public List<Triple<String, String, DataType>> getColumnMetaFull() {
        return columnMetaFull;
    }
    public List<Triple<String, String, DataType>> getColumnMetaSelected() {
        return columnMetaSelected;
    }

    public CdcMetaInfo(EDatabaseType databaseType, String host, int port, String username, String password, String database, String schema, String table,String[] fieldArr) {
        this.databaseType = databaseType;
        this.host = host;
        this.port = port;
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.username = username;
        this.password = password;
        this.fieldArr=fieldArr;
        this.columnMetaFull=new ArrayList<>();
        this.columnMetaSelected=new ArrayList<>();
        if(fieldArr!=null && fieldArr.length>0){
            if(fieldArr.length==1&& "*".equals(fieldArr[0])){
                this.isSingleTableWithSelect=false;
            }else {
                this.isSingleTableWithSelect = true;
            }
        } else {
            this.isSingleTableWithSelect = true;
        }

        initDefaultFields(databaseType);
        buildMetaData(this.database, this.schema, this.table);
    }

    private void initDefaultFields(EDatabaseType databaseType) {
        switch (databaseType) {
            case MySQL:
                defaultDriverName = "com.mysql.cj.jdbc.Driver";
//                defaultDriverName = "com.mysql.jdbc.Driver";
                defaultJdbcUrl = "jdbc:mysql://%s:%d/%s?useSSL=false";
                break;
            case PostgreSQL:
                defaultDriverName = "org.postgresql.Driver";
                defaultJdbcUrl = "jdbc:postgresql://%s:%d/%s";
            default:
                throw new UnsupportedOperationException("unsupported database type:" + databaseType.name());
        }
    }

    public void buildMetaData(String database, String schema, String table) {
//    public void buildMetaData(CdcConf cdcConf, SyncConf syncConf) {
//        List<String> tableList = cdcConf.getTableList();
//        List<FieldConf> fieldList;
//        if (1 == tableList.size()) {
//            fieldList = syncConf.getReader().getFieldList();
//        } else {
//            fieldList = null;
//            throw new IllegalArgumentException("when field is filled,only one table is permitted!");
//        }
//        List<MutableTriple<String,String, String>> tableIdentifyList = tableList.stream().distinct().map(table -> {
//            String[] fullName = table.split("\\.");
//            String tableOrSchemaName = fullName.length == 2 ? fullName[0] : "";
//            String tableName = fullName.length == 2 ? fullName[1] : table;
//            MutableTriple<String,String, String> t3 = new MutableTriple<>();
//            switch(this.databaseType){
//                case MySQL:
//                    t3.setLeft(tableOrSchemaName);
//                    t3.setMiddle("");
//                    t3.setRight(tableName);
//                    break;
//                default:
//                    ;
//            }
//            return t3;
//        }).collect(Collectors.toList());
//        String driverName =this.getDefaultDriverName();
//        try {
//            this.jdbcUrl = cdcConf.getConnection().get(0).obtainJdbcUrl();
//        } catch (Exception e) {
//            LOG.info("Cant get jdbcURL from CdcConf :" + e.getMessage());
//        }
//        for (MutableTriple<String,String, String> triple : tableIdentifyList) {
        this.jdbcUrl = StringUtils.isEmpty(this.jdbcUrl) ? getDefaultJdbcUrl(this.database) : this.jdbcUrl;
        Connection connection = getConnection(this.jdbcUrl, this.getUsername(), this.getPassword());
        Pair<List<String>, List<String>> tableMetaData = JdbcUtil.getTableMetaData(database, schema, table, connection);
        List<String> tableNameList = tableMetaData.getLeft();
        List<String> tableTypeList = tableMetaData.getRight();
        for (int i = 0; i < tableNameList.size(); i++) {
            String tableName = tableNameList.get(i);
            String tableType = tableTypeList.get(i);
            DataType dataType = MysqlCdcRawTypeConverter.apply(tableType);
            columnMetaFull.add(new ImmutableTriple<>(tableName, tableType, dataType));
        }
        // 修改selected column
        for (String field : fieldArr) {
            for (Triple<String, String, DataType> triple : columnMetaFull) {
                if(triple.getLeft().equalsIgnoreCase(field)){
                    columnMetaSelected.add(triple);
                }
            }
        }

    }


    public Connection getConnection(String jdbcUrl, String username, String password) {
        if (StringUtils.isNotBlank(jdbcUrl) && StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            String DRIVER_NAME = this.defaultDriverName;
            ClassUtil.forName(DRIVER_NAME, Thread.currentThread().getContextClassLoader());
            Properties prop = new Properties();
            prop.put("user", username);
            prop.put("password", password);

            synchronized (ClassUtil.LOCK_STR) {
                return RetryUtil.executeWithRetry(
                        () -> DriverManager.getConnection(jdbcUrl, prop),
                        3,
                        2000,
                        false);
            }
        } else
            return null;
    }

    public String getDefaultJdbcUrl(String database) {
        return String.format(defaultJdbcUrl, host, port, database);
    }

    public String getDefaultDriverName() {
        return defaultDriverName;
    }


    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }
}

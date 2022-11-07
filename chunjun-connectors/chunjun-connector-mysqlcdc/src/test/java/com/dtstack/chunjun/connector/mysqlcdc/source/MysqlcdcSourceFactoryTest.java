package com.dtstack.chunjun.connector.mysqlcdc.source;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.common.collect.Lists;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class MysqlcdcSourceFactoryTest {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlcdcSourceFactoryTest.class);
    public static void main(String[] args) throws Exception {

//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("localhost")
//                .port(3306)
//                .databaseList("test") // set captured database
//                .tableList(Lists.newArrayList("test.student").toArray(new String[0])) // set captured table
//                .username("root")
//                .password("123456")
//                .deserializer(new JsonDebeziumDeserializationSchema(true))
//                .startupOptions(StartupOptions.initial())
//                .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"mysqlCDC")
//                .print().setParallelism(4); // use parallelism 1 for sink to keep message ordering
//        env.execute();
    }

}

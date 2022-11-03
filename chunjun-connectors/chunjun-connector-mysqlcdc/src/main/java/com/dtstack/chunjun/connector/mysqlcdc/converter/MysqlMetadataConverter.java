package com.dtstack.chunjun.connector.mysqlcdc.converter;

import com.ververica.cdc.debezium.table.MetadataConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

public class MysqlMetadataConverter implements MetadataConverter {
    @Override
    public Object read(SourceRecord sourceRecord) {
        Integer integer = sourceRecord.kafkaPartition();
        Object key = sourceRecord.key();
        Schema schema = sourceRecord.keySchema();
        Object value = sourceRecord.value();
        Long timestamp = sourceRecord.timestamp();
        return null;
    }
}

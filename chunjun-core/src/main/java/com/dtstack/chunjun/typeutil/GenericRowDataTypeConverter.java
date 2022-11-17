package com.dtstack.chunjun.typeutil;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * @program: ChunJun
 * @author: wuren
 * @create: 2021/04/14
 */
public class GenericRowDataTypeConverter {

    /** 将MySQL数据库中的类型，转换成flink的DataType类型。 转换关系参考 com.mysql.jdbc.MysqlDefs 类里面的信息。 */
    public static AbstractBaseColumn apply(Object v) {
        if (v instanceof Boolean) {
            return new BooleanColumn(((Boolean) v));
        } else if (v instanceof Byte) {
            return new ByteColumn((Byte) v);
        } else if (v instanceof byte[]) {
            return new BytesColumn((byte[]) v);
        } else if (v instanceof StringData) {
            return new StringColumn(((StringData) v).toString());
        } else if (v instanceof DecimalData) {
            return new BigDecimalColumn(((DecimalData) v).toBigDecimal());
        } else if (v instanceof Integer) {
            return new BigDecimalColumn(((Integer) v));
        } else if (v instanceof Long) {
            return new BigDecimalColumn(((Long) v));
        } else if (v instanceof Short) {
            return new BigDecimalColumn(((Short) v));
        } else if (v instanceof Float) {
            return new BigDecimalColumn(((Float) v));
        } else if (v instanceof Double) {
            return new BigDecimalColumn(((Double) v));
        } else if (v instanceof TimestampData) {
            return new TimestampColumn(((TimestampData) v).getMillisecond());
        } else if (v == null) {
            return new NullColumn();
        }
        return new StringColumn(v.toString());
    }
}

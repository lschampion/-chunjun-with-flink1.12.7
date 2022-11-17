package com.dtstack.chunjun.connector.mysqlcdc.enums;

import org.apache.commons.lang3.StringUtils;

public enum UpdateType {
    BEFORE("before"),
    AFTER("after");
    private final String name;

    UpdateType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static String getByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException(
                    "EJobType name cannot be null or empty , just support sql or sync jobType !!! ");
        }
        switch (name) {
            case "sql":
                return BEFORE.name;
            case "sync":
                return AFTER.name;
            default:
                throw new RuntimeException("just support sql or sync jobType !!!");
        }
    }
}

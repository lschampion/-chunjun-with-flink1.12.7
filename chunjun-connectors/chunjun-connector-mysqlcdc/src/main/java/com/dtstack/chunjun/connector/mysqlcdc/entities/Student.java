package com.dtstack.chunjun.connector.mysqlcdc.entities;

//        s_id int,
//        s_name VARCHAR(20) NOT NULL DEFAULT '',
//                s_birth date,
//                s_sex VARCHAR(10) NOT NULL DEFAULT '',
//                c_time datetime,


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {

    public int s_id;
    public String s_name;
    public int s_birth;
    public String s_sex;
    public Long datetime;
}

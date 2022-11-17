show variables like '%lower%';
-- my.cnf 需要设置：lower_case_table_names=1

# 查看是否开启binlog
show variables like '%log_bin%';

drop table if exists student;
-- 学生表
CREATE TABLE student(
s_id int,
s_name VARCHAR(20) NOT NULL DEFAULT '',
s_birth date,
s_sex VARCHAR(10) NOT NULL DEFAULT '',
c_time datetime,
PRIMARY KEY(s_id)
);

-- 插入学生表测试数据
insert into student values('01' , '赵雷' , '1990-01-01' , '男',current_timestamp);
insert into student values('02' , '钱电' , '1990-12-21' , '男',current_timestamp);
insert into student values('03' , '孙风' , '1990-05-20' , '男',current_timestamp);
insert into student values('04' , '李云' , '1990-08-06' , '男',current_timestamp);
insert into student values('05' , '周梅' , '1991-12-01' , '女',current_timestamp);
insert into student values('06' , '吴兰' , '1992-03-01' , '女',current_timestamp);
insert into student values('07' , '郑竹' , '1989-07-01' , '女',current_timestamp);
insert into student values('08' , '王菊' , '1990-01-20' , '女',current_timestamp);
insert into student values('09' , '赵六' , '1999-01-20' , '男',current_timestamp);


update student set s_name='我看看' where s_id='06';
select * from student where s_id='06';

select * from student order by s_id desc limit 10;
select max(s_id) s_id from test.student;
select count(1) sl from test.student s;
select * from STUDENT;



drop table if exists student_sync;
-- 学生表
CREATE TABLE student_sync(
s_id int,
s_name VARCHAR(20) NOT NULL DEFAULT '',
s_birth date,
s_sex VARCHAR(10) NOT NULL DEFAULT '',
c_time datetime,
PRIMARY KEY(s_id)
);

truncate student_sync;
select * from student_sync order by s_id desc limit 10;
select * from student_sync where s_id='06';
select count(1) sl from student_sync;

select * from STUDENT where 1=2


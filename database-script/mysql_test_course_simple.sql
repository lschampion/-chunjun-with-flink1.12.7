


use test;


drop table if exists test.address;
-- 地址
CREATE table test.address(
a_id bigint,
a_name VARCHAR(80) NOT NULL DEFAULT '',
a_city VARCHAR(80),
PRIMARY KEY(a_id)
);




select * from address order by a_id desc limit 10;
select max(a_id) a_id from test.address;
select count(1) sl from test.address a;
select * from test.address;



------------------------------------------------------------------------

drop table if exists test.address_sync;
-- 地址
CREATE table test.address_sync(
a_id bigint,
a_name VARCHAR(80) NOT NULL DEFAULT '',
a_city VARCHAR(80),
PRIMARY KEY(a_id)
);


select * from address_sync order by a_id desc limit 10;
select max(a_id) a_id from test.address_sync;
select count(1) sl from test.address_sync a;
select * from test.address_sync;





-- 权限设置表 只赋权限 和回收权限 创建角色需要管理员去线上创建。不然容易出错和导致数据安全问题
-- 不能放删除表或修改表结构的语句或DML语句
create schema if not exists rtp;
grant usage on schema rtp to dpsquery;
grant select on all tables in schema rtp to dpsquery;
-- 自动获取新增schema表权限
alter default privileges in schema dms grant select on tables to dpsquery;

create schema if not exists dms;
grant usage on schema dms to dpsquery;
grant select on all tables in schema dms to dpsquery;
-- 自动获取新增schema表权限
alter default privileges in schema dms grant select on tables to dpsquery;

create schema if not exists stg;
grant usage on schema stg to dpsquery;
grant select on all tables in schema stg to dpsquery;
alter default privileges in schema stg grant select on tables to dpsquery;

create schema if not exists edw;
grant usage on schema edw to dpsquery;
grant select on all tables in schema edw to dpsquery;
-- 自动获取新增schema表权限
alter default privileges in schema edw grant select on tables to dpsquery;

create schema if not exists ods;
grant usage on schema ods to dpsquery;
grant select on all tables in schema ods to dpsquery;
-- 自动获取新增schema表权限
alter default privileges in schema ods grant select on tables to dpsquery;
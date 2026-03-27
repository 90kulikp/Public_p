--Create table for DDL logs
drop table if exists dbo.DDL_logs;
create table dbo.DDL_logs (
ID int IDENTITY(1,1) PRIMARY KEY,
username varchar(255),
eventtype varchar(20),
schemaname varchar(20),
tablename varchar(255),
eventtime datetime);

--Create table for DML logs
drop table if exists dbo.DML_logs;
create table dbo.DML_logs (
ID int IDENTITY(1,1) PRIMARY KEY,
username varchar(255),
eventtype varchar(50),
rowsaffected int,
schemaname varchar(50),
tablename varchar(255),
eventtime datetime);
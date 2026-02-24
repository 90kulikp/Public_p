--drop existing talbe
drop table if exists dbo.dim_date;

--create dim_date table
create table dbo.dim_date
(Date_SK int IDENTITY(1,1) PRIMARY KEY,
Date date,
Year smallint,
DayOfYear smallint,
Quarter smallint,
QuarterID varchar(6),
Month smallint,
MonthID varchar(7),
MonthName varchar(9),
DayOfMonth smallint,
Week smallint,
DayOfWeek smallint,
DayName varchar(9)
);

declare @dt date, @end_dt date
set @dt = '2022-01-01'                          --set your start date
set @end_dt = cast(getdate() as date)           --set your ending date

while @dt<=@end_dt
begin 

insert into dbo.dim_date 
(Date, Year, DayOfYear, Quarter, QuarterID, Month, MonthID, MonthName, DayOfMonth, Week, DayOfWeek, DayName) 
select 
@dt date_,
year(@dt),
datepart(dayofyear,@dt),
datepart(quarter,@dt),
concat(year(@dt),'Q',datepart(quarter,@dt)),
month(@dt),
concat_ws('-',year(@dt),format(month(@dt),'00')),
datename(month,@dt),
day(@dt),
datepart(week,@dt),
datepart(weekday,@dt),
datename(weekday,@dt)

set @dt = dateadd(day,1,@dt)
end;

--see the results
select top 1000 * from dbo.dim_date;

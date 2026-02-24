drop table if exists stg.currency_rates_cl;

create table stg.currency_rates_cl
(name text, fullcode text, code text, unit_multipler text, exchange_rate text, date date);

insert into stg.currency_rates_cl (name, fullcode, code, unit_multipler, exchange_rate, date)
select 
initcap(nullif(trim("Nazwa waluty"),'')) "Nazwa waluty", 
upper(nullif(trim("Kod waluty"),'')) "Kod waluty", 
upper(nullif(trim(split_part(trim("Kod waluty"),' ',-1)),'')),
nullif(trim(split_part(trim("Kod waluty"),' ',1)),''),
replace("Kurs średni",',','.'), 
cast("Data" as date) 
FROM stg.currency_rates
where 1=1
and stg.is_date("Data");

delete from stg.currency_rates_cl
where 1=0
or name is null
or code is null
or date>current_date
or stg.is_numeric(exchange_rate)=false
or stg.is_numeric(unit_multipler)=false;

delete from stg.currency_rates_cl a
using stg.currency_rates_cl b
where 1=1
and a.date<b.date
and a.code = b.code;

delete from stg.currency_rates_cl a
using stg.currency_rates_cl b
where 1=1
and a.ctid > b.ctid
and a.code = b.code;
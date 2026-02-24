drop table if exists stg.financial_instruments_cl;

SELECT symbol, name, exchange, "assetType", "ipoDate", "delistingDate", status 
into stg.financial_instruments_cl
FROM stg.financial_instruments
where 1=0;

insert into stg.financial_instruments_cl
(symbol, name, exchange, "assetType", "ipoDate", "delistingDate", status)
SELECT 
upper(nullif(trim(symbol),'')) symbol, 
initcap(nullif(trim(name),'')) name, 
upper(nullif(trim(exchange),'')) exchange, 
upper(nullif(trim("assetType"),'')) "assetType", 
nullif(trim("ipoDate"),'') "ipoDate", 
nullif(trim("delistingDate"),'') "delistingDate", 
initcap(nullif(trim(status),'')) status 
FROM stg.financial_instruments;


delete from stg.financial_instruments_cl
where 1=0
or symbol is null
or symbol='symbol'
or name is null
or stg.is_date("ipoDate")=False;

update stg.financial_instruments_cl
set "delistingDate"=null
where stg.is_date("delistingDate")=False;

update stg.financial_instruments_cl
set status='Delisted'
where cast("delistingDate" as date)<=current_date;

delete from stg.financial_instruments_cl a
using stg.financial_instruments_cl b
where a.ctid > b.ctid
and a.symbol = b.symbol;
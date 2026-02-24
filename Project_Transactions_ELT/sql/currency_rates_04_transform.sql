begin;

DO 
'
DECLARE
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stg.currency_rates_cl
        GROUP BY code
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found duplicates in STG'';
    END IF;
    
    IF EXISTS (
    SELECT 1
    FROM stg.currency_rates_cl
    WHERE 1=0
    or code is null
    or unit_multipler is null
    or exchange_rate is null
    ) THEN
        RAISE EXCEPTION ''Found NULLs in STG'';
    END IF;
    
    IF EXISTS (
    SELECT 1
    FROM stg.currency_rates_cl
    WHERE 1=0
    or stg.is_numeric(unit_multipler) = false
    or stg.is_numeric(exchange_rate) = false
    ) THEN
        RAISE EXCEPTION ''found not numeric values in STG'';
    END IF;
END;
'LANGUAGE PLPGSQL;

update rpt.dim_currency_rates 
set 
record_current=false
from rpt.dim_currency_rates D
inner join rpt.dim_currencies C on D.currency_sk=C.currency_sk
where 1=1
and D.record_current
and exists (select 'a' from stg.currency_rates_cl S
                where 1=1
                and C.currency_id=S.code
                and (
                D.date<S.date
                     or (D.date=S.date
                     and D.unitvalue<>cast(S.exchange_rate as float)/cast(S.unit_multipler as smallint)))
                );



insert into rpt.dim_currency_rates
(currency_sk, date, unitvalue, record_current, record_importdate)
select
C.currency_sk,
date,
cast(exchange_rate as float)/cast(unit_multipler as smallint),
true,
current_date
from stg.currency_rates_cl S
inner join rpt.dim_currencies C on S.code=C.currency_id and C.record_current
where 1=1
and not exists (select 'a' from rpt.dim_currency_rates D
                inner join rpt.dim_currencies E on D.currency_sk=E.currency_sk
                where 1=1
                and E.currency_id=S.code
                and D.record_current);
                

DO '
DECLARE
BEGIN
    IF EXISTS (
        SELECT currency_sk, date
        FROM rpt.dim_currency_rates
        WHERE record_current 
        GROUP BY currency_sk, date
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found multiple current records in DIM'';
    END IF;
    
END ;
'LANGUAGE PLPGSQL;

commit;
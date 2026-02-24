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
    WHERE code IS NULL
    ) THEN
        RAISE EXCEPTION ''Found NULLs in STG'';
    END IF;
END;
'LANGUAGE PLPGSQL;

update rpt.dim_currencies D
set 
record_current=false,
record_enddate=current_date-1
where 1=1
and record_current
and exists (select 'a' from stg.currency_rates_cl S
                where 1=1 
                and S.code=D.currency_id
                and S.name<>D.name);


insert into rpt.dim_currencies
(currency_id, name, record_current, record_startdate, record_enddate)
select 
code,
name,
true, current_date, '9999-12-31'
 from stg.currency_rates_cl S
where 1=1
and not exists (select 'a' from rpt.dim_currencies D
                where 1=1 
                and S.code=D.currency_id
                and D.record_current);

DO '
DECLARE
BEGIN
    IF EXISTS (
        SELECT currency_id
        FROM rpt.dim_currencies
        WHERE record_current = true
        GROUP BY currency_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found multiple current records in DIM'';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rpt.dim_currencies a
        JOIN rpt.dim_currencies b
          ON a.currency_id = b.currency_id
         AND a.currency_sk <> b.currency_sk
         AND daterange(a.record_startdate, a.record_enddate, ''[]'')
         && daterange(b.record_startdate, b.record_enddate, ''[]'')
    ) THEN
        RAISE EXCEPTION ''Found overlapping date ranges in DIM'';
    END IF;
END ;
'LANGUAGE PLPGSQL;


commit;
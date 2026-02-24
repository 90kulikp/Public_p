begin;

DO 
'
DECLARE
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stg.financial_instruments_cl
        GROUP BY symbol
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found duplicates in STG'';
    END IF;
    
    IF EXISTS (
    SELECT 1
    FROM stg.financial_instruments_cl
    WHERE symbol IS NULL 
    ) THEN
        RAISE EXCEPTION ''Found NULLs in STG'';
    END IF;
END;
'LANGUAGE PLPGSQL;

update rpt.dim_financial_instruments D
set 
record_current=false,
record_enddate=current_date-1
where 1=1
and record_current
and exists (select 'a' from stg.financial_instruments_cl S 
                where 1=1
                and D.financialinstrument_id=S.symbol
                and (D.name<>S.name or
                D.exchange<>S.exchange or
                D.assetType<>S."assetType" or
                cast(D.ipoDate as text)<>S."ipoDate" or
                cast(D.delistingDate as text)<>S."delistingDate" or
                D.status<>S.status)
                );
                
                
insert into rpt.dim_financial_instruments           
(financialinstrument_id, name, exchange, assetType, ipoDate, delistingDate, status, record_current, record_startdate, record_enddate)
select
symbol, name, exchange, "assetType", 
cast(S."ipoDate" as date), cast("delistingDate" as date), status, 
true, current_date, '9999-12-31'
FROM stg.financial_instruments_cl S
where 1=1
and not exists (select 'a' from rpt.dim_financial_instruments D 
                where 1=1 
                and D.financialinstrument_id=S.symbol
                and D.record_current);
               
DO '
DECLARE
BEGIN
    IF EXISTS (
        SELECT financialinstrument_id
        FROM rpt.dim_financial_instruments
        WHERE record_current = true
        GROUP BY financialinstrument_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found multiple current records in DIM'';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rpt.dim_financial_instruments a
        JOIN rpt.dim_financial_instruments b
          ON a.financialinstrument_id = b.financialinstrument_id
         AND a.financialinstrument_sk <> b.financialinstrument_sk
         AND daterange(a.record_startdate, a.record_enddate, ''[]'')
         && daterange(b.record_startdate, b.record_enddate, ''[]'')
    ) THEN
        RAISE EXCEPTION ''Found overlapping date ranges in DIM'';
    END IF;
END ;
'LANGUAGE PLPGSQL;

commit;
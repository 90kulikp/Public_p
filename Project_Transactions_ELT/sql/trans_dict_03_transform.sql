begin;

DO 
'
DECLARE
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stg.trans_dict_cl
        GROUP BY name, "Dictionary"
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found duplicates in STG'';
    END IF;
    
    IF EXISTS (
    SELECT 1
    FROM stg.trans_dict_cl
    WHERE 1=0
    or name is null
    or "Dictionary" is null
    ) THEN
        RAISE EXCEPTION ''Found NULLs in STG'';
    END IF;
END;
'LANGUAGE PLPGSQL;

update rpt.dim_transaction_dict D
set 
record_current=false,
record_enddate=current_date-1
where 1=1
and record_current
and exists (select 'a' from stg.trans_dict_cl S
                where 1=1
                and S."Dictionary"=D.dictionary
                and S.name=D.item_id
                and (S.full_name<>D.name or
                S.description<>D.description));
            
                
insert into rpt.dim_transaction_dict 
(dictionary, item_id, name, description, record_current, record_startdate, record_enddate)
select 
"Dictionary", name, full_name, description,
true,current_date,'9999-12-31'
from stg.trans_dict_cl S
where 1=1
and name is not null
and not exists (select 'a' from rpt.dim_transaction_dict D
                where 1=1
                and S."Dictionary"=D.dictionary
                and S.name=D.item_id
                and D.record_current);
                

DO '
DECLARE
BEGIN
    IF EXISTS (
        SELECT item_id
        FROM rpt.dim_transaction_dict
        WHERE record_current = true
        GROUP BY dictionary, item_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found multiple current records in DIM'';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rpt.dim_transaction_dict a
        JOIN rpt.dim_transaction_dict b
          ON a.item_id = b.item_id
         AND a.dictionary = b.dictionary
         AND a.item_sk <> b.item_sk
         AND daterange(a.record_startdate, a.record_enddate, ''[]'')
         && daterange(b.record_startdate, b.record_enddate, ''[]'')
    ) THEN
        RAISE EXCEPTION ''Found overlapping date ranges in DIM'';
    END IF;
END ;
'LANGUAGE PLPGSQL;

commit;
begin;

DO 
'
DECLARE
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stg.transaction_file_cl
        GROUP BY client_pid, client_name, client_lastname, client_origin
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found duplicates in STG'';
    END IF;
    
    IF EXISTS (
    SELECT 1
    FROM stg.transaction_file_cl
    WHERE client_pid IS NULL
    ) THEN
        RAISE EXCEPTION ''Found NULLs in STG'';
    END IF;
END;
'LANGUAGE PLPGSQL;

update rpt.dim_clients 
set record_current=false,
record_enddate=current_date-1
from rpt.dim_clients D
left join rpt.dim_countries B on D.country_sk=B.country_sk
where 1=1
and D.record_current
and exists (select 'a' from stg.transaction_file_cl T 
                where 1=1
                and D.client_id=T.client_pid
                and (D.client_name<>T.client_name OR
                     D.client_lastname<>T.client_lastname OR
                     B.name<>T.client_origin
                        ));

insert into rpt.dim_clients
(client_name, client_lastname, client_id, country_sk, record_current, record_startdate, record_enddate)
select 
S.client_name, S.client_lastname, S.client_pid, C.country_sk,
true, current_date, '9999-12-31'
from stg.transaction_file_cl S
left join rpt.dim_countries C on S.client_origin=C.name and C.record_current='Y'
where 1=1
and not exists (select 'a' from rpt.dim_clients R 
                where 1=1
                and S.client_pid=R.client_id
                and R.record_current);

DO '
DECLARE
BEGIN
    IF EXISTS (
        SELECT client_id
        FROM rpt.dim_clients
        WHERE record_current
        GROUP BY client_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found multiple current records in DIM'';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rpt.dim_clients a
        JOIN rpt.dim_clients b
          ON a.client_id = b.client_id
         AND a.client_sk <> b.client_sk
         AND daterange(a.record_startdate, a.record_enddate, ''[]'')
         && daterange(b.record_startdate, b.record_enddate, ''[]'')
    ) THEN
        RAISE EXCEPTION ''Found overlapping date ranges in DIM'';
    END IF;
END ;
'LANGUAGE PLPGSQL;


commit;
begin;

DO 
'
DECLARE
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stg.countries_cl
        GROUP BY code
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found duplicates in STG'';
    END IF;
    
    IF EXISTS (
    SELECT 1
    FROM stg.countries_cl
    WHERE code IS NULL --tutaj wszystkie kolumny, ktore nie mogą być NULLami
    ) THEN
        RAISE EXCEPTION ''Found NULLs in STG'';
    END IF;
END;
'LANGUAGE PLPGSQL;

update rpt.dim_countries D
set 
record_current=false,
record_enddate=current_date-1
where 1=1
and record_current=true
and exists (select 'a' from stg.countries_cl S where S.code=D.Country_ID and S.name<>D.name);

---

insert into rpt.dim_countries
(Country_ID, name, record_current, record_startdate, record_enddate)
select
code, name, true, current_date, '9999-12-31'
from stg.countries_cl S
where 1=1
and S.code is not null
and not exists (select 'a' from rpt.dim_countries D 
                where 1=1
                and S.code=D.Country_ID 
                and D.record_current);

DO '
DECLARE
BEGIN
    IF EXISTS (
        SELECT country_id
        FROM rpt.dim_countries
        WHERE record_current 
        GROUP BY country_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION ''Found multiple current records in DIM'';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rpt.dim_countries a
        JOIN rpt.dim_countries b
          ON a.country_id = b.country_id
         AND a.country_sk <> b.country_sk
         AND daterange(a.record_startdate, a.record_enddate, ''[]'')
         && daterange(b.record_startdate, b.record_enddate, ''[]'')
    ) THEN
        RAISE EXCEPTION ''Found overlapping date ranges in DIM'';
    END IF;
END ;
'LANGUAGE PLPGSQL;

commit;
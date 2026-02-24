drop table if exists stg.countries_cl;

select code, name, geonameid into stg.countries_cl
from stg.countries
where 1=0;

insert into stg.countries_cl (code, name, geonameid)
select 
upper(nullif(trim(code),'')) code,
initcap(nullif(trim(name),'')) name,
nullif(geonameid,0) geonameid
from stg.countries;

delete from stg.countries_cl
where 1=0
or code is null
or name is null;

delete from stg.countries_cl a
using stg.countries_cl b
where a.ctid > b.ctid
and a.code = b.code;
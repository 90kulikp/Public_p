drop table if exists stg.trans_dict_cl;

select 
name, full_name, description, "Dictionary" 
into stg.trans_dict_cl
FROM stg.trans_dict
where 1=0;

insert into stg.trans_dict_cl
(name, full_name, description, "Dictionary")
select
upper(nullif(trim(name),'')) name, 
upper(nullif(trim(full_name),'')) full_name, 
lower(nullif(trim(description),'')) description, 
initcap(nullif(trim("Dictionary"),'')) "Dictionary"
FROM stg.trans_dict;

delete from stg.trans_dict_cl
where 1=0
or name is null
or full_name is null
or "Dictionary" is null;

delete from stg.trans_dict_cl a
using stg.trans_dict_cl b
where 1=1
and a.ctid > b.ctid
and a.name = b.name
and a."Dictionary"=b."Dictionary";
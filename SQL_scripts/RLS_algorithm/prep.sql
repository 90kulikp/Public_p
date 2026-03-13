--creating source table
drop table if exists [dbo].[manager_empoyee_relations];
create table [dbo].[manager_empoyee_relations] (manager varchar(200), employee varchar(200));

--filling source table with sample data
insert into [dbo].[manager_empoyee_relations] values ('person_1','person_2');
insert into [dbo].[manager_empoyee_relations] values ('person_1','person_3');
insert into [dbo].[manager_empoyee_relations] values ('person_2','person_4');
insert into [dbo].[manager_empoyee_relations] values ('person_2','person_5');
insert into [dbo].[manager_empoyee_relations] values ('person_2','person_6');
insert into [dbo].[manager_empoyee_relations] values ('person_3','person_7');
insert into [dbo].[manager_empoyee_relations] values ('person_3','person_8');
insert into [dbo].[manager_empoyee_relations] values ('person_3','person_9');
insert into [dbo].[manager_empoyee_relations] values ('person_6','person_10');
insert into [dbo].[manager_empoyee_relations] values ('person_6','person_11');
insert into [dbo].[manager_empoyee_relations] values ('person_6','person_12');
insert into [dbo].[manager_empoyee_relations] values ('person_9','person_13');
insert into [dbo].[manager_empoyee_relations] values ('person_9','person_14');

--creating destination table;
drop table if exists [dbo].[RLS];
create table [dbo].[RLS] (privilege_owner varchar(200), data_owner varchar(200));

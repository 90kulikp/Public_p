--creating a cursor to iterate through each manager in the source table
declare @manager varchar(200) 

declare cursor_m cursor FOR
select distinct manager from [dbo].[manager_empoyee_relations]

open cursor_m

FETCH NEXT FROM CURSOR_M into @manager
while @@FETCH_STATUS=0

BEGIN

--creating temporary tables used during processing for a single manager
declare @result_table table (login_ varchar(200)) -- stores all employees found in the manager's hierarchy
declare @actual_check table (login_ varchar(200)) -- stores logins that will be checked in the current internal iteration
declare @loop_check table (login_ varchar(200))  -- support table, used to refill @actual_check for the next internal iteration

--creating variable controlling the internal loop execution
declare @counter int 
set @counter = (select count(*) from [dbo].[manager_empoyee_relations] where manager=@manager)

--inserting the current manager login to start searching for downstream relations
insert into @actual_check 
select @manager

--internal loop
while @counter>0
begin
--adding employees managed by the current level to the result table
insert into @result_table
select employee from [dbo].[manager_empoyee_relations] where manager in (select login_ from @actual_check)
--preparing data for the next iteration
delete from @loop_check
insert into @loop_check
select login_ from @actual_check

delete from @actual_check
--loading employees of the previous level as managers to check in the next loop
insert into @actual_check 
select employee from [dbo].[manager_empoyee_relations] where manager in (select login_ from @loop_check)
--update loop condition
set @counter = (select count(*) from @actual_check)
--end of internal loop
end

--filling target table with permissions for manager
insert into [dbo].[RLS]
select @manager, login_ from @result_table

--clearing temporary tables before processing the next manager
delete from @result_table
delete from @actual_check

--fetching next manager
FETCH NEXT FROM CURSOR_M into @manager
--cursor end
END
close cursor_m
deallocate cursor_m

--grant users permission to view their own data
insert into [dbo].[RLS]
select distinct manager, manager from [dbo].[manager_empoyee_relations]
union
select distinct employee, employee from [dbo].[manager_empoyee_relations];

--display results
select * from [dbo].[RLS];
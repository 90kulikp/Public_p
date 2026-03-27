
DROP TRIGGER IF EXISTS [DDL_TRIGGER] ON DATABASE;
--Create DDL trigger for 'create table', 'alter table' and 'drop table' events
CREATE TRIGGER [DDL_TRIGGER]
ON DATABASE
FOR CREATE_TABLE, ALTER_TABLE, DROP_TABLE 
AS 
BEGIN
--Declare variables to store event details
        DECLARE @data xml, @schemaName varchar(50), @objectName varchar(255), @eventType varchar(50)
--Retrieve event data in XML format        
        SET @data = EVENTDATA()
--Extract schema name, object name, and event type from XML
        SET @schemaName = @data.value('(/EVENT_INSTANCE/SchemaName)[1]','varchar(20)')
        SET @objectName = @data.value('(/EVENT_INSTANCE/ObjectName)[1]','varchar(255)')
        SET @eventType = @data.value('(/EVENT_INSTANCE/EventType)[1]','varchar(20)')
--Insert DDL event details into the log table
        insert into dbo.DDL_logs (username, eventtype, schemaname, tablename, eventtime) values
        (system_user, @eventType, @schemaName, @objectName, getdate())
--Create a DML trigger for newly created or altered tables         
        IF @eventType in ('CREATE_TABLE','ALTER_TABLE') 
        BEGIN
--Declare variables for full table name and DML trigger name         
                DECLARE @fullname varchar(255), @triggername varchar(255)
                SET @fullname = @schemaName+'.'+@objectName
                SET @triggername = @fullname + '_dml_trigger'
--Drop existing DML trigger (needed after ALTER TABLE)      
                DECLARE @deletetriggercmd varchar(500)
                SET @deletetriggercmd = 'DROP TRIGGER IF EXISTS '+@triggername+'; '
                EXEC (@deletetriggercmd)
--Build dynamic SQL statement to create DML trigger  
                DECLARE @dmltriggercmd varchar(2000)
                SET @dmltriggercmd = '
                CREATE TRIGGER '+@triggername+'
                ON '+@fullname+'
                AFTER INSERT, UPDATE, DELETE
                AS BEGIN
--Determine DML operation type: I (INSERT), U (UPDATE), D (DELETE) 
                DECLARE @Action as char(1)
                SET @Action = (CASE WHEN EXISTS(SELECT * FROM INSERTED)
                                AND EXISTS(SELECT * FROM DELETED)
                                THEN ''U''
                                WHEN EXISTS(SELECT * FROM INSERTED)
                                THEN ''I''
                                WHEN EXISTS(SELECT * FROM DELETED)
                                THEN ''D''
                                ELSE NULL
                                END)
--Retrieve table name and schema for the current trigger        
                DECLARE @tablename varchar(255), @schemaname varchar(20)
                SET @tablename = (SELECT OBJECT_NAME(parent_object_id)
                                FROM sys.objects
                                WHERE object_id = @@PROCID)
                SET @schemaname = (SELECT S.name
                                FROM sys.objects O
                                INNER JOIN sys.schemas S on O.schema_id=S.schema_id
                                WHERE O.object_id = @@PROCID)
--Calculate the number of affected rows                      
                DECLARE @rowCount int
                SET @rowCount = (CASE WHEN @Action in (''I'',''U'') THEN 
                                (SELECT COUNT(*) FROM INSERTED)
                                WHEN @Action = ''D'' THEN 
                                (SELECT COUNT(*) FROM DELETED) END)
--Insert DML event details into the log table    
                INSERT INTO dbo.DML_logs (username, eventtype, schemaname, tablename, eventtime, rowsaffected) VALUES
                (system_user, @Action, @schemaname, @tablename, getdate(), @rowCount)
        
                END'
--Execute dynamic SQL to create the DML trigger
                EXEC (@dmltriggercmd)
        
        END
        
END;
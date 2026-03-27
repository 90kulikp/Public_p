# DDL&DML logging triggers

## Tech stack
MS SQL Server 2022

## Description
This code creates a database-level DDL trigger that monitors table structure changes such as creating, altering, or dropping tables and logs these events. When a table is created or modified, it automatically generates a DML trigger for that table. This DML trigger tracks data changes (INSERT, UPDATE, DELETE) and records information about the operation, including the type of action and the number of affected rows. This allows tracking both table schema changes and data modifications in the database.

## Files
- prep.sql - create tables for logs
- triggers.sql - main algorithm
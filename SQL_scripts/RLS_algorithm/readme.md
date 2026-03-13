# RLS algorithm. Flattening hierarchical relations

## Tech stack
MS SQL Server 2022

## Description
This SQL script flattens hierarchical manager–employee relationships stored in a table with two columns: Manager and Employee, where each row represents a direct reporting relationship.

The script expands the hierarchy to generate all indirect relationships between managers and their subordinates across every level of the structure.

The result is a flattened permission table containing two fields:

Privilege Owner – the user who has access rights

Data Owner – the user whose data can be accessed

Each manager is mapped to all employees within their reporting hierarchy, while employees are also mapped to their own data.

This flattened structure can be used directly to implement Row Level Security (RLS) rules in Power BI.

## Files
- Prep.sql - creating tables and filling with sample data
- RLS_algorithm.sql - main algorithm
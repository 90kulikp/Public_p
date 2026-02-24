# Transactions ELT 


## Introducion
This project shows data extraction from multiple different sources, load into RDBMS and transform into star schema.

Project was made on Debian 12 with on prem installation of: Apache Airflow, Python (with libraries), PostgreSQL, MongoDB.


## Business context
Let’s assume we work for a stock brokerage company. We need to store data in a data warehouse (DWH) from a transaction platform where clients buy and sell shares. Unfortunately, the only way to extract data from the system is through a flat file containing denormalized data, so additional data sources are required to better describe the facts


## Features
* Different data sources: REST API, Web Scrapping, MongoDB, CSV file
* Apache Airflow orchestration
* Taskflow DAG
* Staging area
* Data cleansing
* Data validation
* Slowly Changing Dimensions (SCD) type 2
* Indexes, primary and foreign keys
* User defined functions (UDF) - wrote in Python, used in PostgreSQL
* Star schema data normalization
* Unit tests
* GitHub Actions workflow


## Tech stack
* Python
* Pandas
* PostgreSQL
* Apache Airflow
* GitHub Actions


## Project structure

**Project_Transactions_ELT\\**\
|\
|--- **sample_inputs_and_refs\\** > sample data or references to external data sources\
|--- **dag\\** > airflow DAG\
|--- **raw\\** > raw CSV file to import\
|--- **sql\\** > SQL scripts\
|--- **python\\** > Python scripts\
|--- **test\\** > tests for python scripts\
|--- **info\\** > aditional info (see below)

Info folder contains additional info about project:
* Airflow Connections in YAML file
* DAG graphical presentation in PNG file with legend in TXT file
* Entity Relationship Diagram (ERD)
* Data Flow Diagram (DFD)
* SQL file with target database preparation scripts: create database, create schema, create table, create index and create function statements
* requirements TXT file with list of python libraries


## Future improvements
* Data visualisation dashboard
* Dynamic Data Masking

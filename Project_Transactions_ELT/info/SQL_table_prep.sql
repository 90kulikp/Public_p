CREATE DATABASE transactions_project;

/*
Insert your username.
*/
GRANT ALL PRIVILEGES ON DATABASE transactions_project TO myusername;  
/*
Manually connect to new db "transactions_project", before executing code below. PostgreSQL 15.15 doesn't have possibility to change database using SQL.
*/

CREATE SCHEMA stg;
CREATE SCHEMA rpt;

-- dim_clients
CREATE TABLE dim_clients 
(client_sk BIGSERIAL NOT NULL, 
client_name TEXT, 
client_lastname TEXT, 
client_id BIGINT NOT NULL, 
country_sk INTEGER, 
record_current BOOLEAN NOT NULL, 
record_startdate DATE NOT NULL, 
record_enddate DATE NOT NULL, 
PRIMARY KEY (client_sk), 
CONSTRAINT dim_clients_country_sk_fkey FOREIGN KEY (country_sk) REFERENCES "dim_countries" ("country_sk"), 
CONSTRAINT date_range_validation CHECK (record_startdate <= record_enddate));

CREATE INDEX i_dim_clients_between_dates 
ON dim_clients (client_id, record_startdate, record_enddate);

CREATE INDEX i_dim_clients_lookup 
ON dim_clients (client_id, record_current);

CREATE UNIQUE INDEX u_dim_clients_record_current 
ON dim_clients (client_id) WHERE (record_current = true);

-- dim_countries
CREATE TABLE dim_countries 
(country_sk SERIAL NOT NULL, 
country_id TEXT NOT NULL, 
name TEXT, 
record_current BOOLEAN NOT NULL, 
record_startdate DATE NOT NULL, 
record_enddate DATE NOT NULL, 
PRIMARY KEY (country_sk), 
CONSTRAINT date_range_validation CHECK (record_startdate <= record_enddate));

CREATE INDEX i_dim_countries_between_dates 
ON dim_countries (country_id, record_startdate, record_enddate);

CREATE INDEX i_dim_countries_lookup 
ON dim_countries (country_id, record_current);

CREATE UNIQUE INDEX u_dim_countries_record_current 
ON dim_countries (country_id) WHERE (record_current = true);

-- dim_currencies
CREATE TABLE dim_currencies 
(currency_sk SERIAL NOT NULL, 
currency_id TEXT NOT NULL, 
name TEXT, 
record_current BOOLEAN NOT NULL, 
record_startdate DATE NOT NULL, 
record_enddate DATE NOT NULL, 
PRIMARY KEY (currency_sk), 
CONSTRAINT date_range_validation CHECK (record_startdate <= record_enddate));

CREATE INDEX i_dim_currencies_between_dates 
ON dim_currencies (currency_id, record_startdate, record_enddate);

CREATE INDEX i_dim_currencies_lookup 
ON dim_currencies (currency_id, record_current);

CREATE UNIQUE INDEX u_dim_currencies_record_current 
ON dim_currencies (currency_id) WHERE (record_current = true);

-- dim_currency_rates
CREATE TABLE dim_currency_rates 
(currency_rate_sk SERIAL NOT NULL, 
currency_sk INTEGER NOT NULL, 
date DATE, 
unitvalue NUMERIC(10,8), 
record_current BOOLEAN, 
record_importdate DATE, 
PRIMARY KEY (currency_rate_sk), 
CONSTRAINT dim_currency_rates_currency_sk_fkey FOREIGN KEY (currency_sk) REFERENCES "dim_currencies" ("currency_sk"));

CREATE INDEX i_dim_currency_rates_dates 
ON dim_currency_rates (currency_sk, date);

CREATE INDEX i_dim_currency_rates_lookup 
ON dim_currency_rates (currency_sk, record_current);

CREATE UNIQUE INDEX u_dim_currency_rates_record_current 
ON dim_currency_rates (currency_sk) WHERE (record_current = true);

-- dim_financial_instruments
CREATE TABLE dim_financial_instruments 
(financialinstrument_sk BIGSERIAL NOT NULL, 
financialinstrument_id TEXT NOT NULL, 
name TEXT, 
exchange TEXT, 
assettype TEXT, 
ipodate DATE, 
delistingdate DATE, 
status TEXT, 
record_current BOOLEAN NOT NULL, 
record_startdate DATE NOT NULL, 
record_enddate DATE NOT NULL, 
PRIMARY KEY (financialinstrument_sk), 
CONSTRAINT date_range_validation CHECK (record_startdate <= record_enddate));

CREATE INDEX i_dim_financial_instruments_between_dates 
ON dim_financial_instruments (financialinstrument_id, record_startdate, record_enddate);

CREATE INDEX i_dim_financial_instruments_lookup 
ON dim_financial_instruments (financialinstrument_id, record_current);

CREATE UNIQUE INDEX u_dim_financial_instruments_record_current 
ON dim_financial_instruments (financialinstrument_id) WHERE (record_current = true);

-- dim_transaction_dict
CREATE TABLE dim_transaction_dict 
(item_sk SERIAL NOT NULL, 
dictionary TEXT NOT NULL, 
item_id CHARACTER(3) NOT NULL, 
name TEXT, 
description TEXT, 
record_current BOOLEAN NOT NULL, 
record_startdate DATE NOT NULL, 
record_enddate DATE NOT NULL, 
PRIMARY KEY (item_sk), 
CONSTRAINT date_range_validation CHECK (record_startdate <= record_enddate));

CREATE INDEX i_dim_transaction_dict_between_dates 
ON dim_transaction_dict (item_id, dictionary, record_startdate, record_enddate);

CREATE INDEX i_dim_transaction_dict_lookup 
ON dim_transaction_dict (item_id, dictionary, record_current);

CREATE UNIQUE INDEX u_dim_transaction_dict_record_current 
ON dim_transaction_dict (item_id, dictionary) WHERE (record_current = true);

-- fct_transactions
CREATE TABLE fct_transactions 
(transaction_sk BIGSERIAL NOT NULL, 
financialinstrument_sk BIGINT, 
country_sk INTEGER, 
currency_sk INTEGER, 
trade_date DATE, 
execution_time TEXT, 
side TEXT, 
quantity DOUBLE PRECISION, 
price_per_unit DOUBLE PRECISION, 
gross_amount DOUBLE PRECISION, 
commission DOUBLE PRECISION, 
tax DOUBLE PRECISION, 
net_amount DOUBLE PRECISION, 
client_sk BIGINT, 
transactiontype_sk INTEGER, 
channel_sk INTEGER, 
created_at TEXT, 
updated_at TEXT, 
record_insertdate DATE NOT NULL, 
PRIMARY KEY (transaction_sk), 
CONSTRAINT fct_transactions_financialinstrument_sk_fkey FOREIGN KEY (financialinstrument_sk) REFERENCES "dim_financial_instruments" ("financialinstrument_sk"), 
CONSTRAINT fct_transactions_country_sk_fkey FOREIGN KEY (country_sk) REFERENCES "dim_countries" ("country_sk"), 
CONSTRAINT fct_transactions_currency_sk_fkey FOREIGN KEY (currency_sk) REFERENCES "dim_currencies" ("currency_sk"), 
CONSTRAINT fct_transactions_client_sk_fkey FOREIGN KEY (client_sk) REFERENCES "dim_clients" ("client_sk"), 
CONSTRAINT fct_transactions_transactiontype_sk_fkey FOREIGN KEY (transactiontype_sk) REFERENCES "dim_transaction_dict" ("item_sk"), 
CONSTRAINT fct_transactions_channel_sk_fkey FOREIGN KEY (channel_sk) REFERENCES "dim_transaction_dict" ("item_sk"));

CREATE INDEX i_fct_transactions_insert_date 
ON fct_transactions (record_insertdate);

CREATE INDEX i_fct_transactions_trade_date 
ON fct_transactions (trade_date);

--functions
CREATE FUNCTION is_date (x text)  RETURNS boolean
  VOLATILE
AS $body$
  from datetime import datetime
  try:
    datetime.strptime(x, '%Y-%m-%d')
    return 1
  except:
    return 0
$body$ LANGUAGE plpython3u;

CREATE FUNCTION is_numeric (x text)  RETURNS boolean
  VOLATILE
AS $body$
  try:
    int(x)
    return 1
  except:
    try:
        float(x)
        return 1
    except:
        return 0
$body$ LANGUAGE plpython3u;



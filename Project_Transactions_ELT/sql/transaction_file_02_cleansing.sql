drop table if exists stg.transaction_file_cl;

select instrument_type, ticker_symbol, exchange, country, currency, trade_date, execution_time, side, quantity, price_per_unit, gross_amount, commission, tax, net_amount, client_pid, client_name, client_lastname, client_origin, transaction_type, channel, created_at, updated_at
into stg.transaction_file_cl
FROM stg.transaction_file
where 1=0;

insert into stg.transaction_file_cl
(instrument_type, ticker_symbol, exchange, country, currency, trade_date, execution_time, side, quantity, price_per_unit, gross_amount, commission, tax, net_amount, client_pid, client_name, client_lastname, client_origin, transaction_type, channel, created_at, updated_at)
select
upper(nullif(trim(instrument_type),'')) instrument_type, 
upper(nullif(trim(ticker_symbol),'')) ticker_symbol,
upper(nullif(trim(exchange),'')) exchange, 
initcap(nullif(trim(country),'')) country, 
upper(nullif(trim(currency),'')) currency, 
trade_date, 
execution_time, 
left(upper(nullif(trim(side),'')),1) side, 
quantity, 
price_per_unit, 
gross_amount, 
commission, 
tax, 
net_amount, 
client_pid, 
initcap(nullif(trim(client_name),'')) client_name, 
initcap(nullif(trim(client_lastname),'')) client_lastname, 
initcap(nullif(trim(client_origin),'')) client_origin, 
upper(nullif(trim(transaction_type),'')) transaction_type, 
upper(nullif(trim(channel),'')) channel, 
created_at, 
updated_at 
FROM stg.transaction_file;


delete from stg.transaction_file_cl
where 1=0
or ticker_symbol is null
or stg.is_numeric(cast(quantity as text))=false
or stg.is_numeric(cast(price_per_unit as text))=false
or client_pid is null
or stg.is_date(trade_date)=false;
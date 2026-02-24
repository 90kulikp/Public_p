begin;

DO 
'
DECLARE
BEGIN
    IF EXISTS (
        select 1
        from stg.transaction_file_cl T
        where 1=0
        or not exists (select 1 from rpt.dim_financial_instruments S where T.ticker_symbol=S.financialinstrument_id and S.record_current)
        or not exists (select 1 from rpt.dim_countries C where T.country=C.name and C.record_current)
        or not exists (select 1 from rpt.dim_currencies D where T.currency=D.currency_id and D.record_current)
        or not exists (select 1 from rpt.dim_clients E where T.client_pid=E.client_id and E.record_current)
        or not exists (select 1 from rpt.dim_transaction_dict F where T.transaction_type=F.name and F.dictionary=''Types'' and F.record_current)
        or not exists (select 1 from rpt.dim_transaction_dict G where T.channel=G.name and G.dictionary=''Channels'' and G.record_current)
    ) THEN
        RAISE EXCEPTION ''Found uncorrelated values in STG'';
    END IF;
    

END;
'LANGUAGE PLPGSQL;


insert into rpt.fct_transactions
(financialinstrument_sk, country_sk, currency_sk, trade_date, execution_time, side, quantity, price_per_unit, gross_amount, commission, tax, net_amount, client_sk, transactiontype_sk, channel_sk, created_at, updated_at, record_insertdate)

select
S.financialinstrument_sk,
C.country_sk,
D.currency_sk,
cast(T.trade_date as date),
T.execution_time,
T.side,
T.quantity,
T.price_per_unit,
T.gross_amount,
T.commission,
T.tax,
T.net_amount,
E.client_sk,
F.item_sk transactiontypeid,
G.item_sk channelid,
T.created_at,
T.updated_at,
current_date record_insertdate
from stg.transaction_file_cl T
inner join rpt.dim_financial_instruments S on T.ticker_symbol=S.financialinstrument_id and S.record_current
inner join rpt.dim_countries C on T.country=C.name and C.record_current
inner join rpt.dim_currencies D on T.currency=D.currency_id and D.record_current
inner join rpt.dim_clients E on T.client_pid=E.client_id and E.record_current
inner join rpt.dim_transaction_dict F on T.transaction_type=F.name and F.dictionary='Types' and F.record_current
inner join rpt.dim_transaction_dict G on T.channel=G.name and G.dictionary='Channels' and G.record_current
;

commit; 
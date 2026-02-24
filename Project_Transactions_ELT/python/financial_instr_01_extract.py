import pandas as pd
import io



def financial_instr_EL(restapi_conn,postgres_conn, BaseHook, HttpHook, PostgresHook):
	#from airflow.sdk.bases.hook import BaseHook
	#from airflow.providers.http.hooks.http import HttpHook
	#from airflow.providers.postgres.hooks.postgres import PostgresHook

	conn_headers = BaseHook.get_connection(restapi_conn).extra_dejson.get("headers", {})

	hook_ac = HttpHook(method="GET",http_conn_id=restapi_conn)
	response_ac = hook_ac.run(
        endpoint="/query",
        headers=conn_headers,
        data={
        "function": "LISTING_STATUS",
        "state": "active"
    	}
    )
	response_ac.raise_for_status()
		
	hook_no = HttpHook(method="GET",http_conn_id=restapi_conn)
	response_no = hook_no.run(
        endpoint="/query",
        headers=conn_headers,
        data={
        "function": "LISTING_STATUS",
        "state": "delisted"
    	}
    )
	response_no.raise_for_status()
	print(response_no.url)
	response_full = response_ac.content.decode('utf-8') +  response_no.content.decode('utf-8')
	response_csv = io.StringIO(response_full,newline="\n")
	df = pd.read_csv(response_csv)

	postgres_engine = PostgresHook(postgres_conn_id=postgres_conn).get_sqlalchemy_engine()
	df.to_sql('financial_instruments',schema='stg',con=postgres_engine,if_exists='replace',index=False)

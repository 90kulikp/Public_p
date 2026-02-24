import pandas as pd

def countries_EL(restapi_conn, postgres_conn, BaseHook, HttpHook, PostgresHook):
    #from airflow.sdk.bases.hook import BaseHook
    #from airflow.providers.http.hooks.http import HttpHook
    #from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    conn_headers = BaseHook.get_connection(restapi_conn).extra_dejson.get("headers", {})
    hooka = HttpHook(method="GET",http_conn_id=restapi_conn)
    response = hooka.run(
        endpoint="/countries",
        headers=conn_headers,
        extra_options={
            "params": {
                "format": "json",
                "language": "en"
            }
        }
    )
    response.raise_for_status()
    data = response.json()["countries"]
    df = pd.DataFrame(data)
    
    postgres_engine = PostgresHook(postgres_conn_id=postgres_conn).get_sqlalchemy_engine()
    
    df.to_sql(name='countries',schema='stg',con=postgres_engine,if_exists='replace',index=False)
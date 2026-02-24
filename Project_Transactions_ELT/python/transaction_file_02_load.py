import pandas as pd
from sqlalchemy import text

def trans_file_L(files_list, postgres_conn, PostgresHook): 
    #from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    files_to_import = files_list   

    if files_to_import and len(files_to_import)>1:
        postgres_engine = PostgresHook(postgres_conn_id=postgres_conn).get_sqlalchemy_engine()   
        drop_query = "drop table if exists stg.transaction_file;"
        with postgres_engine.connect() as conn:
            conn.execute(text(drop_query))
            conn.execute(text("COMMIT;"))

        for i in files_to_import:
            df = pd.read_csv(i,sep='|')
            df.to_sql('transaction_file',schema='stg',con=postgres_engine,if_exists='append',index=False)    

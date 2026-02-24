import pandas as pd


def dict_EL(mongo_conn, postgres_conn, MongoHook, PostgresHook):
    #from airflow.providers.mongo.hooks.mongo import MongoHook
    #from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = MongoHook(mongo_conn_id=mongo_conn)
    client = hook.get_conn()
    db = client.get_database()

    coursor = db['dictT'].find({"SourceSystem": "Transactions", "Dictionary": {"$in":["Types","Channels"]}, "inUse": 1})
    list_cur = list(coursor)

    df = pd.json_normalize(list_cur,"Values","Dictionary")

    postgres_engine = PostgresHook(postgres_conn_id=postgres_conn).get_sqlalchemy_engine()
    df.to_sql('trans_dict',schema='stg',con=postgres_engine,if_exists='replace',index=False)

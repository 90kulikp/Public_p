from bs4 import BeautifulSoup
import pandas as pd
import regex as re


def currency_rates_EL(http_conn, postgres_conn, HttpHook, PostgresHook):
    #from airflow.providers.http.hooks.http import HttpHook
    #from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = HttpHook(method="GET",http_conn_id=http_conn)

    response = hook.run(
        endpoint="/statystyka-i-sprawozdawczosc/kursy/tabela-a/"
    )

    response.raise_for_status()
    html = response.text

    tree = BeautifulSoup(html,"lxml")
    table_tag = tree.select("table")[0]
    tab_data = [[item.text for item in row_data.select("th,td")]
                for row_data in table_tag.select("tr")]

    df = pd.DataFrame(tab_data[1:], columns=tab_data[0])

    r_data = re.search(" z dnia 20[2-9][0-9]-[0-1][0-9]-[0-3][0-9]", html).span()
    df["Data"] = html[r_data[0]:r_data[1]][-10:]

    postgres_engine = PostgresHook(postgres_conn_id=postgres_conn).get_sqlalchemy_engine()
    df.to_sql('currency_rates',schema='stg',con=postgres_engine,if_exists='replace',index=False)

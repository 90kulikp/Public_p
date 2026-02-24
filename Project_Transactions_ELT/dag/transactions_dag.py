from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.configuration import conf
from datetime import datetime
import sys
from pathlib import PurePath

parent_dir = PurePath(__file__).parents[1]
script_dir = parent_dir / 'python'
sql_dir = parent_dir / 'sql'
sys.path.append(str(script_dir))

from countries_01_extract import countries_EL
from financial_instr_01_extract import financial_instr_EL
from currency_rates_01_extract import currency_rates_EL
from trans_dict_01_extract import dict_EL
from transaction_file_01_select_files import trans_file_E
from transaction_file_02_load import trans_file_L
from transaction_file_03_archivization import trans_file_archive

@dag(
    dag_id="transactions_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transactions", "stock","python","sql","rest_api","web_scrapping","postgresql","mongodb","flat_file"],
)

def dag_transactions():

    @task.python
    def countries_1_EL():
        countries_EL(restapi_conn='world-geo-data_countries',postgres_conn='postgres_pr_tran',BaseHook=BaseHook, HttpHook=HttpHook, PostgresHook=PostgresHook)
    countries1_EL = countries_1_EL()

    @task.python
    def fin_instruments_1_EL():
        financial_instr_EL(restapi_conn='alpha-vantage_listing_status',postgres_conn='postgres_pr_tran',BaseHook=BaseHook, HttpHook=HttpHook, PostgresHook=PostgresHook)
    f_instruments1_EL = fin_instruments_1_EL()

    @task.python
    def currency_rates_1_EL():
        currency_rates_EL(http_conn='nbp_currency_scraper',postgres_conn='postgres_pr_tran', HttpHook=HttpHook, PostgresHook=PostgresHook)
    currency_rates1_EL = currency_rates_1_EL()

    @task.python
    def dict_1_EL():
        dict_EL(mongo_conn='mongo_pr1',postgres_conn='postgres_pr_tran', MongoHook=MongoHook, PostgresHook=PostgresHook)
    dict1_EL = dict_1_EL()

    @task(do_xcom_push=True)
    def transaction_file_1_E():
        return trans_file_E()
    transaction_file1_E = transaction_file_1_E()

    @task.short_circuit
    def transaction_file_2_V(**context):
        trans_files = context["ti"].xcom_pull(task_ids="transaction_file_1_E", key="return_value")
        return (trans_files and len(trans_files)>0)
    transaction_file2_V = transaction_file_2_V()

    @task.python
    def transaction_file_3_L(**context):
        trans_files = context["ti"].xcom_pull(task_ids="transaction_file_1_E", key="return_value")
        trans_file_L(files_list=trans_files, postgres_conn='postgres_pr_tran', PostgresHook=PostgresHook)
    transaction_file3_L = transaction_file_3_L()

    @task
    def transaction_file_4_A(**context):
        trans_files = context["ti"].xcom_pull(task_ids="transaction_file_1_E", key="return_value")
        trans_file_archive(trans_files)
    transaction_file4_A = transaction_file_4_A()

    @task
    def countries_2_C():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "countries_02_cleansing.sql") as f:
            sql = f.read()
        hook.run(sql)
    countries2_C = countries_2_C()

    @task
    def countries_3_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "countries_03_transform.sql") as f:
            sql = f.read()
        hook.run(sql)
    countries3_T = countries_3_T()

    @task
    def countries_4_D():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "countries_04_drop.sql") as f:
            sql = f.read()
        hook.run(sql)
    countries4_D = countries_4_D()

    @task
    def currency_rates_2_C():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "currency_rates_02_cleansing.sql") as f:
            sql = f.read()
        hook.run(sql)
    currency_rates2_C = currency_rates_2_C()

    @task
    def currency_rates_3_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "currency_rates_03_transform.sql") as f:
            sql = f.read()
        hook.run(sql)
    currency_rates3_T = currency_rates_3_T()

    @task
    def currency_rates_4_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "currency_rates_04_transform.sql") as f:
            sql = f.read()
        hook.run(sql)
    currency_rates4_T = currency_rates_4_T()

    @task
    def currency_rates_5_D():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "currency_rates_05_drop.sql") as f:
            sql = f.read()
        hook.run(sql)
    currency_rates5_D = currency_rates_5_D()

    @task
    def fin_instruments_2_C():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "financial_instr_02_cleansing.sql") as f:
            sql = f.read()
        hook.run(sql)
    f_instruments2_C = fin_instruments_2_C()     

    @task
    def fin_instruments_3_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "financial_instr_03_transform.sql") as f:
            sql = f.read()
        hook.run(sql)
    f_instruments3_T = fin_instruments_3_T() 

    @task
    def fin_instruments_4_D():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "financial_instr_03_transform.sql") as f:
            sql = f.read()
        hook.run(sql)
    f_instruments4_D = fin_instruments_4_D() 

    @task
    def dict_2_C():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "trans_dict_02_cleansing.sql") as f:
            sql = f.read()
        hook.run(sql)
    dict2_C = dict_2_C()

    @task
    def dict_3_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "trans_dict_03_transform.sql") as f:
            sql = f.read()
        hook.run(sql)
    dict3_T = dict_3_T()

    @task
    def dict_4_D():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "trans_dict_04_drop.sql") as f:
            sql = f.read()
        hook.run(sql)
    dict4_D = dict_4_D()

    @task
    def transaction_file_4_C():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "transaction_file_02_cleansing.sql") as f:
            sql = f.read()
        hook.run(sql)
    transaction_file4_C = transaction_file_4_C()

    @task
    def transaction_file_5_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "transaction_file_03_transform_clients.sql") as f:
            sql = f.read()
        hook.run(sql)
    transaction_file5_T = transaction_file_5_T()
    
    @task
    def transaction_file_6_T():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "transaction_file_04_transform_transactions.sql") as f:
            sql = f.read()
        hook.run(sql)
    transaction_file6_T = transaction_file_6_T()

    @task
    def transaction_file_6_D():
        hook = PostgresHook(postgres_conn_id='postgres_pr_tran')
        with open(sql_dir / "transaction_file_05_drop.sql") as f:
            sql = f.read()
        hook.run(sql)
    transaction_file6_D = transaction_file_6_D()


    countries1_EL >> countries2_C >> countries3_T >> countries4_D
    currency_rates1_EL >> currency_rates2_C >> currency_rates3_T >> currency_rates4_T >> currency_rates5_D
    f_instruments1_EL >> f_instruments2_C >> f_instruments3_T >> f_instruments4_D
    dict1_EL >> dict2_C >> dict3_T >> dict4_D
    transaction_file1_E >> transaction_file2_V >> transaction_file3_L >> [transaction_file4_A, transaction_file4_C] 
    transaction_file4_C >> transaction_file5_T 
    [countries3_T, currency_rates4_T, f_instruments3_T, dict3_T, transaction_file5_T ] >> transaction_file6_T >> transaction_file6_D

dag_transactions()
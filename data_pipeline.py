from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from sqlalchemy import create_engine
from datetime import timedelta
import requests
import pandas as pd
import json
import os
import uuid
import ast

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A data pipeline DAG',
    schedule_interval='0 8 * * *',
    start_date=datetime(2024, 8, 30),
    catchup=False,
)


# Job 1: INGEST DATA
def ingest_data():
    url = 'https://restcountries.com/v3.1/all'
    response = requests.get(url)
    data = response.json()
    os.makedirs('raw', exist_ok=True)
    with open('raw/countries.json', 'w') as f:
        json.dump(data, f)

# ingest_data()

# Job 2: EXTRACT DATA
def extract_data():
    with open('raw/countries.json', 'r') as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    os.makedirs('foundation', exist_ok=True)
    df.to_csv('foundation/countries.csv',index=False)

# extract_data()

def parse_json_like_string(value):
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return value

def generate_quoc_gia_id(name_dict, namespace_uuid):
    if isinstance(name_dict, dict):
        common_name = name_dict.get('common', '')
        official_name = name_dict.get('official', '')
        combined_string = f"{common_name}{official_name}"
        return uuid.uuid5(namespace_uuid, combined_string)
    return None 

# Job 3: TRANSFORM DATA

def transform_data():
    df = pd.read_csv('foundation/countries.csv', sep=',')
    df = df.map(parse_json_like_string)

    namespace_uuid = uuid.UUID("9a5963f8-5a5c-4b8c-aa46-1068af074546")

    df['capital'] = df['capital'].apply(lambda x: "|".join(x) if isinstance(x, list) else x)
    df['altSpellings'] = df['altSpellings'].apply(lambda x: "|".join(x) if isinstance(x, list) else x)
    df['kinh_do'] = df['latlng'].apply(lambda x: x[0] if isinstance(x, list) else x)
    df['vi_do'] = df['latlng'].apply(lambda x: x[1] if isinstance(x, list) else x)
    df['su_dung_tieng_anh'] = df['languages'].notna()
    df['maps'] = df['maps'].apply(lambda x: x.get('googleMaps', '') if isinstance(x, dict) else '')
    df['timezones'] = df['timezones'].apply(lambda x: x[0] if isinstance(x, list) else x)
    df['flags'] = df['flags'].apply(lambda x: x.get('svg', '') if isinstance(x, dict) else '')

    df.rename(columns={
    'ccn3': 'ma_quoc_gia',
    'cca3': 'ma_quoc_gia_3',
    'independent': 'doc_lap',
    'status': 'tinh_trang',
    'capital': 'thu_do',
    'altSpellings': 'ten_khac',
    'region': 'vung',
    'area': 'dien_tich',
    'maps': 'ban_do',
    'timezones': 'mui_gio',
    'continents': 'luc_dia',
    'flags': 'co',
    'startOfWeek': 'bat_dau_tuan'
    }, inplace=True)

    df['quoc_gia_id'] = df['name'].apply(lambda x: generate_quoc_gia_id(x, namespace_uuid))

    # Separate currency data
    currency_df = df[['quoc_gia_id', 'currencies']].copy()
    currency_df = currency_df.explode('currencies')

    df.drop(columns=['currencies'], inplace=True)
    print(currency_df, "CURRENCY DF")

    # Save to trusted folders
    os.makedirs('trusted/countries', exist_ok=True)
    os.makedirs('trusted/country_currency', exist_ok=True)
    df.to_csv('trusted/countries/countries.csv', index=False)
    currency_df.to_csv('trusted/country_currency/currencies.csv', index=False)

# transform_data()

# Job 4: LOAD DATA
def load_data():
    connection_string = 'mysql+mysqlconnector://admin:123@localhost/local'
    engine = create_engine(connection_string)

    # Load countries data
    df_countries = pd.read_csv('trusted/countries/countries.csv', sep=',')

    with engine.connect():
        df_countries.to_sql('countries', con=engine, if_exists='replace', index=False)

    # Load currencies data
    df_currencies = pd.read_csv('trusted/country_currency/currencies.csv', sep=',')

    with engine.connect():
        df_currencies.to_sql('country_currency', con=engine, if_exists='replace', index=False)

# load_data()


# Define tasks
task_ingest = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

task_extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Define task sequence
task_ingest >> task_extract >> task_transform >> task_load

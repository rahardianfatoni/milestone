'''
=================================================
Milestone 3

Nama  : Rahardiansyah Fatoni 
Batch : FTDS-027-RMT

The objective is to simulate automating the process of fetching data prom PostgreSQL, cleaning it, and then posting it to Elasticsearch using Airflow.
=================================================
'''

import pandas as pd 
import psycopg2 as db 
from elasticsearch import Elasticsearch
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def queryPostgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)
    df=pd.read_sql("SELECT * from public.table_m3",conn)
    print(df.head())
    df.to_csv('P2M3_rahardiansyah_fatoni_data_raw.csv')
    return df

def clean_data():
    df=pd.read_csv('P2M3_rahardiansyah_fatoni_data_raw.csv')
    df.columns=[x.lower() for x in df.columns] 
    df.columns=[x.replace(" ", "_") for x in df.columns]
    df.columns=[x.strip() for x in df.columns]
    df = df.dropna() 
    print("Cleaned data:")
    print(df.head()) 
    print(df.info())
    df.to_csv('P2M3_rahardiansyah_fatoni_data_clean.csv', index=False)

def post_elastic():
    es = Elasticsearch('http://elasticsearch:9200')  
    df=pd.read_csv('P2M3_rahardiansyah_fatoni_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="p2m3_rahardiansyah_fatoni_videogame_sales", doc_type="doc", body=doc)
        print(res) 

default_args = { 
    'owner': 'ardi',
    'start_date': dt.datetime(2024, 2, 24, 23) - timedelta(7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),    
}

with DAG('P2M3_rahardiansyah_fatoni_DAG',
         default_args=default_args,
         schedule_interval="@daily",      # '0 * * * *',
         catchup=False) as dag:

    fetchData = PythonOperator(task_id='fetch_from_postgres',
                                 python_callable=queryPostgresql())
    
    cleanData = PythonOperator(task_id='clean',
                                 python_callable=clean_data)

    postElastic = PythonOperator(task_id='post_elastic',
                                 python_callable=post_elastic)

fetchData >> cleanData >> postElastic





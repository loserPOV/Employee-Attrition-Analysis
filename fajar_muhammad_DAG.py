'''
=======================================
Name : Fajar Ibrah Muhammad

Objective : Program ini dibuat untuk automasi pembersihan/pengubahan data dan load data melalui postgre 
            hingga di upload ke dalam elasticsearch untuk nantinya dilakukan visualisasi menggunakan kibana.

Dataset : https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset

docker-compose -f airflow_windows.yaml up -d
=======================================
'''

# import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def load_csv_to_postgres():
    '''
    membuat csv raw menjadi tabel di postgresql menggunakan sql alchemy
    '''
    database = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine = create_engine("postgresql+psycopg2://{username}:{password}@{host}/{database}")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_fajar_muhammad_data_raw.csv')
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  # Menggunakan if_exists='replace' agar tabel digantikan jika sudah ada
      
def data_fetch():
    # fetch data
    '''
    mengambil dataset melalui tabel postgresql > menjadikan dataframe > dikembalikan men jadi csv data baru
    '''
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_fajar_muhammad_data_new.csv', sep=',', index=False)


def preprocessing(): 
    '''
    membaca data baru kemudian melakukan preprocessing sebagai berikut:
    - drop kolom: EmployeeCount > hanya berisi satu value (1)
                  Over18 > semua berusia diatas 18
                  DailyRate,HourlyRate,MonthlyRate > sudah diwakilkan MonthlyIncome
                  StandardHours > hanya berisi 1 value (80)
                  StockOptionLevel > tidak ada penjelasan tentang masing2 level
    - drop null
    - drop duplikat
    - mengubah nama kolom menjadi lowercase
    
    membuat csv baru berisikan data bersih 
    '''
    data = pd.read_csv("/opt/airflow/dags/P2M3_fajar_muhammad_data_new.csv")

    # Drop specific columns
    columns_to_drop = ['EmployeeCount', 'Over18', 'DailyRate', 'HourlyRate', 'MonthlyRate', 
                       'StandardHours', 'StockOptionLevel']  # Replace 'Column1' and 'Column2' with the names of the columns you want to drop
    data.drop(columns=columns_to_drop, inplace=True)

    # Clean data 
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)

    # Change all column names to lowercase
    data.columns = data.columns.str.lower()

    # Save the cleaned data to a new CSV file
    data.to_csv('/opt/airflow/dags/P2M3_fajar_muhammad_data_clean.csv', index=False)


def upload_to_elasticsearch():
    # atur
    '''
    meng-upload data bersih ke elasticsearch secara bulk untuk nantinya di visualisasikan dalam kibana
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_fajar_muhammad_data_clean.csv')

    # Persiapkan data untuk pengindeksan paket besar
    actions = [
        {"_op_type": "index", "_index": "table_m3", "_id": i+1, "_source": doc.to_dict()}
        for i, doc in df.iterrows()
    ]

    # Gunakan bulk indexing untuk mengirimkan data sekaligus
    success, failed = bulk(es, actions=actions, index="table_m3")

    print(f"Successfully indexed: {success}")

default_args = {
    'owner': 'Fajar',
    'start_date': datetime(2023, 12, 24, 12, 00)
}

with DAG(
    "P2M3_Fajar_M_DAG", #atur sesuai nama project
    description='Milestone_3',
    schedule_interval='30 6 * * *', #atur schedule
    default_args=default_args, 
    catchup=False
) as dag:
    # Task 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuai dengan nama function yang dibuat diatas
    
    # Task 2
    '''  Fungsi ini ditujukan untuk menjalankan ambil data dari postgresql. '''
    fetching_data = PythonOperator(
        task_id='fetching_data',
        python_callable=data_fetch)
    
    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)
    
    # Task: 4
    '''  Fungsi ini ditujukan untuk menjalankan upload data ke elasticsearch.'''
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)
    
    
    #proses/step penjalanan task airflow
    load_csv_task >> fetching_data >> edit_data >> upload_data
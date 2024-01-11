from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import requests
import logging
import json
import time

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 11, 1, 00, 00)
}

def get_api_data() -> requests.Response:
    response = requests.get('https://api.coincap.io/v2/assets')
    
    return response.json()

def transform_data(res) -> dict:
    for crypto in res['data']:
        crypto.update({"datetime": str(datetime.today())})
    
    return res

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            response = transform_data(get_api_data())
            
            producer.send('crypto_added', json.dumps(response).encode('utf-8'))
        except Exception as e:
            logging.error(f"ERROR: {e}")
            continue

with DAG('crypto_automation', default_args=default_args, schedule='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(task_id='stream_data_from_api', python_callable=stream_data, op_args=dag)

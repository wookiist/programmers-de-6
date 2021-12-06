from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta, date
import requests
import logging


def get_redshift_cur():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def extract(**context):
    url = context['params']['url']
    lat = context['params']['lat']
    lon = context['params']['lon']
    exclude = context['params']['exclude']
    appid = context['params']['api_key']

    task_instance = context['task_instance']
    execution_date = context['execution_date']

    f = requests.get(url, \
            params={'lat': lat, 'lon': lon, 'exclude': exclude, 'appid': appid, 'units': 'metric'})
    
    return (f.json())

def transform(**context):
    raw_data = context['task_instance'].xcom_pull(key='return_value', task_ids='extract')
    transformed_data = []
    for daily in raw_data['daily']: 
        temp_data = {
            'dt': date.fromtimestamp(daily['dt']).strftime('%Y-%m-%d'),
            'temp': daily['temp']['day'],
            'min': daily['temp']['min'],
            'max': daily['temp']['max']
        }
        transformed_data.append(temp_data)
    
    return transformed_data

def load_stage1(**context):
    schema = context['params']['schema']
    table = context['params']['table']
    temp_table = context['params']['temp_table']

    cur = get_redshift_cur()
    sql = f'BEGIN; DROP TABLE IF EXISTS {schema}.{temp_table};'
    sql += f'CREATE TABLE {schema}.{temp_table} AS SELECT * FROM {schema}.{table};END;'
    logging.info(sql)
    cur.execute(sql)

def load_stage2(**context):
    schema = context['params']['schema']
    temp_table = context['params']['temp_table']
    daily_temps = context['task_instance'].xcom_pull(key='return_value', task_ids='transform')

    cur = get_redshift_cur()
    sql = 'BEGIN;'
    for daily_temp in daily_temps:
        dt = daily_temp['dt']
        temp = daily_temp['temp']
        min_ = daily_temp['min']
        max_ = daily_temp['max']
        sql += f'''INSERT INTO {schema}.{temp_table} VALUES ('{dt}', '{temp}', '{min_}', '{max_}');'''
    sql += 'END;'
    logging.info(sql)
    cur.execute(sql)

def load_stage3(**context):
    schema = context['params']['schema']
    table = context['params']['table']
    temp_table = context['params']['temp_table']

    cur = get_redshift_cur()
    sql = f'BEGIN; DELETE FROM {schema}.{table};'
    sql += f'INSERT INTO {schema}.{table} '
    sql += 'SELECT date, temp, min_temp, max_temp FROM ('
    sql += 'WITH tmp AS ('
    sql += f'SELECT *, ROW_NUMBER() OVER(PARTITION BY date ORDER BY created_date DESC) seq FROM {schema}.{temp_table}'
    sql += ') SELECT * FROM tmp WHERE seq=1);END;'
    logging.info(sql)
    cur.execute(sql)

# INSERT INTO kyle_oh95.weather_forecast SELECT date, temp, min_temp, max_temp, created_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq FROM kyle_oh95.temp_weather_forecast) WHERE seq = 1;


dag_daily_weather = DAG(
    dag_id = 'daily_weather',
    start_date = datetime(2021,12,4),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    default_args= {
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get('weather_endpoint'),
        'api_key': Variable.get('weather_api_key'),
        'lat': 37,
        'lon': 126,
        'exclude': 'current,minutely,hourly,alerts'
    },
    provide_context=True,
    dag = dag_daily_weather
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag = dag_daily_weather
)

load_stage1 = PythonOperator(
    task_id='load_stage1',
    python_callable=load_stage1,
    params={
        'schema': 'kyle_oh95',
        'table': 'weather_forecast',
        'temp_table': 'temp_weather_forecast'
    },
    provide_context=True,
    dag = dag_daily_weather
)

load_stage2 = PythonOperator(
    task_id='load_stage2',
    python_callable=load_stage2,
    params={
        'schema': 'kyle_oh95',
        'temp_table': 'temp_weather_forecast'
    },
    provide_context=True,
    dag = dag_daily_weather
)

load_stage3 = PythonOperator(
    task_id='load_stage3',
    python_callable=load_stage3,
    params={
        'schema': 'kyle_oh95',
        'table': 'weather_forecast',
        'temp_table': 'temp_weather_forecast'
    },
    provide_context=True,
    dag = dag_daily_weather
)

extract >> transform >> load_stage1 >> load_stage2 >> load_stage3
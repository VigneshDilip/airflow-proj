from airflow.decorators import dag,task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime 
from include.stock_market.tasks import _get_stock_prices,_store_stock_prices
import requests




@dag(

start_date=datetime(2024, 1, 1),
schedule='@daily',
description='A simple dag to check random numbers',
catchup=False

)


def stock_market():
    
  @task.sensor(poke_interval=30 , timeout=360, mode='poke')

  def is_stock_api_available() -> PokeReturnValue :
    conn=BaseHook.get_connection('stock_api')
    url=f"{conn.host}{conn.extra_dejson['endpoint']}"
    response=requests.get(url,headers=conn.extra_dejson['headers'])
    condition=response.json()["finance"]["result"] is None
    return PokeReturnValue(is_done=condition , xcom_value=url)

  get_stock_prices=PythonOperator(

    task_id='_get_stock_prices',
    python_callable=_get_stock_prices,
    op_kwargs={'url':'{{ti.xcom_pull(task_ids="is_stock_api_available")}}','symbol':'NVDA'}
  )

  store_stock_prices=PythonOperator(

    task_id='_store_stock_prices',
    python_callable=_store_stock_prices,
    op_kwargs={'stocks':'{{ti.xcom_pull(task_ids="_get_stock_prices") }}'}

  )

  is_stock_api_available() >> get_stock_prices >> store_stock_prices


stock_market()  
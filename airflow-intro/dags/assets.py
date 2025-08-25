import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import asset
from airflow.operators.python import PythonOperator


@asset(
    name="user_data_asset",
    schedule="@daily",
    uri="https://randomuser.me/api/"
)

def user_data_asset(self) -> dict[str, any]:

    response = requests.get(self.uri)
    if response.status_code == 200:
        user_data = response.json()
        return user_data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


# def create_asset(**kwargs):
#     asset_name = kwargs.get('asset_name', 'default_asset')
#     print(f"Creating asset: {asset_name}")

# default_args = {
#     'start_date': datetime(2024, 6, 1),
# }

# with DAG(
#     dag_id='create_assets_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=['assets'],
# ) as dag:

#     create_asset_task = PythonOperator(
#         task_id='create_asset_task',
#         python_callable=create_asset,
#         op_kwargs={'asset_name': 'my_asset'},
#     )
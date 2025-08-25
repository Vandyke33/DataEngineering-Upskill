import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import asset, Asset, Context
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
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

@asset(
    schedule=user_data_asset,
)
def user_location(user_data_asset: Asset, context: Context) -> dict[str, any]:
    user_data = context["ti"].xcom_pull(
        dag_id=user_data_asset.name, 
        task_ids=user_data_asset.name, 
        include_prior_dates=True,
    )
    return user_data['results'][0]['location']

@asset(
    schedule=user_data_asset,
)
def user_login(user_data_asset: Asset, context: Context) -> dict[str, any]:
    user_login = context["ti"].xcom_pull(
        dag_id=user_data_asset.name, 
        task_ids=user_data_asset.name, 
        include_prior_dates=True,
    )
    return user_login['results'][0]['login']

@asset.multi(
    schedule=user_data_asset,
    outlets=[Asset(name="user_location"), 
             Asset(name="user_login")],
    # assets=[user_location, user_login],
)
def user_info(user_data_asset: Asset, context: Context) -> list[dict[str, any]]:
    user_data = context["ti"].xcom_pull(
        dag_id=user_data_asset.name, 
        task_ids=user_data_asset.name, 
        include_prior_dates=True,
    )
    location = user_data['results'][0]['location']
    login = user_data['results'][0]['login']
    return [location, login]

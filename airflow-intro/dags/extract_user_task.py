import requests
from typing import Any, Dict
from datetime import datetime
import csv
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


@dag
def user_processing_eu():

    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "postgres",
        sql = """ CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) 
        """        
    )

    @task.sensor(poke_interval=10, timeout=60*5, mode="reschedule")
    def is_api_available() -> PokeReturnValue:
        try:
            response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
            print(response.status_code)
            if response.status_code == 200:
                condition = True
                fake_user = response.json()
            else:
                condition = False
                fake_user = None
            return PokeReturnValue(is_done=condition, xcom_value=fake_user)
        
        except requests.RequestException as e:
            return PokeReturnValue(False, f"Request failed: {e}")
    
    @task    
    def extract_user(fake_user) -> Dict[str, Any]:
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"]
        }
    
    @task
    def store_user(user_info: Dict[str, Any]):
        user_info = {
            "id": 1234,
            "firstname": "Shubham",
            "lastname": "Patil",
            "email": "email@gmail.com"
        }
        print(f"Storing user info: {user_info}")
        # Here you would add logic to store the user info in a database
        with open('/tmp/user_info.csv', mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
    
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    store_user(user_info)

user_processing_eu()
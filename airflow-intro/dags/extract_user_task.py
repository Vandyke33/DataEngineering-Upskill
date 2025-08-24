import requests
from typing import Any, Dict
from datetime import datetime
import csv
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag
def user_processing_eu():

    create_table = SQLExecuteQueryOperator(
        task_id = "create_table_task",
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
    def process_user(user_info: Dict[str, Any]):
        user_info = {
            "id": 1234,
            "firstname": "Shubham",
            "lastname": "Patil",
            "email": "email@gmail.com"
        }
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Storing user info: {user_info}")
        # Here you would add logic to store the user info in a database
        with open('/tmp/user_info.csv', mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")

        staging_table = "users_staging"
        # 1. Ensure staging table exists (same schema as users)
        hook.run(f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (LIKE users INCLUDING ALL);
                """)

        # clean before load
        hook.run("TRUNCATE TABLE users_staging;")

        # 2. Load CSV into staging table
        hook.copy_expert(
            sql=f"COPY {staging_table} FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )

        # 3. Merge (UPSERT) into target table
        hook.run(f"""
            INSERT INTO users (id, firstname, lastname, email)  -- add all columns you need
            SELECT id, firstname, lastname, email
            FROM {staging_table}
            ON CONFLICT (id) DO UPDATE
            SET firstname = EXCLUDED.firstname,
                lastname  = EXCLUDED.lastname,
                email     = EXCLUDED.email;
        """)

    process_user(extract_user(create_table >> is_api_available())) >> store_user()

    # fake_user = is_api_available()
    # user_info = extract_user(fake_user)
    # process_user(user_info)
    # store_user()

user_processing_eu()
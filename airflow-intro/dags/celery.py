from airflow.sdk import task, dag

@dag(
    dag_id="celery_dag",)
def celery_dag():
    @task
    def start():
        sleep(5)
        return "StArT"

    @task
    def process_a(data):
        sleep(5)
        return data.upper()

    @task
    def process_b(data):
        sleep(5)
        return data.lower()

    @task
    def end(data):
        sleep(5)
        print(f"End data: {data}")

    start() >> [process_a(), process_b()] >> end()
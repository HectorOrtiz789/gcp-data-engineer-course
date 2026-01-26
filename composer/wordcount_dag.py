from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineer',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'wordcount_dataflow',
    default_args=default_args,
    description='Ejecutar job de Dataflow desde template',
    schedule_interval=None,
    catchup=False,
)

dataflow_task = DataflowTemplatedJobStartOperator(
    task_id='ejecutar_wordcount_dataflow',
    template = 'gs://gcs-bucket-curso-05-prueba/templates/wordcount_template',
    location='us-central1',
    project_id='ordinal-rig-485316-r2',
    dag=dag,
)
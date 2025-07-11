from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    dag_id='dataflow_streaming_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_dataflow_flex_template = DataflowStartFlexTemplateOperator(
        task_id="run_streaming_pipeline",
        project_id="ancient-cortex-465315-t4",
        location="us-central1",  # <-- Added location here
        body={
            "launchParameter": {
                "jobName": "streaming-dataflow-job",
                "containerSpecGcsPath": "gs://ancient-cortex-465315-t4-raw-data/templates/dataflow_template.json",
                "environment": {
                    "tempLocation": "gs://ancient-cortex-465315-t4-raw-data/temp/"
                },
                "parameters": {
                    "input_topic": "projects/ancient-cortex-465315-t4/topics/stream-topic",
                    "output_table": "ancient-cortex-465315-t4:streaming_output.user_actions",
                    "output_path": "gs://ancient-cortex-465315-t4-raw-data/archive/output"
                }
            }
        },
    )

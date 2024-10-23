from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
import os
from datetime import datetime

version = 1
# Define la ruta al directorio donde están las notebooks
globant_pipeline_path = os.path.dirname(__file__)

# Definir el DAG
with DAG(
    dag_id="globant_pipeline",
    start_date=datetime(2023, 10, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_bronze_notebook = PapermillOperator(
        task_id='run_bronze_notebook',
        input_nb=os.path.join(globant_pipeline_path, 'load_bronze_data.ipynb'),
        output_nb=os.path.join(globant_pipeline_path, 'output_load_bronze_data.ipynb')  # archivo de salida
    )

    run_silver_notebook = PapermillOperator(
        task_id='run_silver_notebook',
        input_nb=os.path.join(globant_pipeline_path, 'load_silver_data.ipynb'),
        output_nb=os.path.join(globant_pipeline_path, 'output_load_silver_data.ipynb')  # archivo de salida
    )

    run_gold_notebook = PapermillOperator(
        task_id='run_gold_notebook',
        input_nb=os.path.join(globant_pipeline_path, 'load_gold_data.ipynb'),
        output_nb=os.path.join(globant_pipeline_path, 'output_load_gold_data.ipynb')  # archivo de salida
    )

    # Definir el flujo de tareas
    run_bronze_notebook >> run_silver_notebook >> run_gold_notebook

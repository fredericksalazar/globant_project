from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Ruta a los archivos CSV
CSV_PATH = "/Users/fsalazars/Documents/Proyectos/globant/prueba tecnica/desarrollo/data/"
globant_pipeline_path = '/Users/fsalazars/airflow/dags/globant_pipeline/'

# Detalles de conexión a PostgreSQL
POSTGRES_CONN_STRING = "postgresql://postgres:123456@127.0.0.1:5432/postgres"  # Reemplaza con tu conexión

version = 1

# Función para cargar los CSV a PostgreSQL usando pandas.to_sql con chunksize
def load_csv_to_postgres_with_pandas(table_name, csv_file, batch_size=1000, **context):
    # Crear el motor de conexión SQLAlchemy
    engine = create_engine(POSTGRES_CONN_STRING)

    # Leer el archivo CSV en Pandas
    df = pd.read_csv(csv_file)

    # Usar pandas.to_sql para cargar en la tabla especificada en lotes de 1000
    df.to_sql(table_name, engine, schema='bronze', if_exists='append', index=False, chunksize=batch_size)

# Definir el DAG
with DAG(
    dag_id="globant_pipeline",
    start_date=datetime(2023, 10, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_bronze_notebook = PapermillOperator(
        task_id = 'run_bronze_notebook',
        input_nb = globant_pipeline_path+'load_bronze_data.ipynb',
        output_nb = globant_pipeline_path+'load_bronze_data.ipynb'
    )

    run_silver_notebook = PapermillOperator(
        task_id = 'run_silver_notebook',
        input_nb = globant_pipeline_path+'load_silver_data.ipynb',
        output_nb = globant_pipeline_path+'load_silver_data.ipynb'
    )

    run_gold_notebook = PapermillOperator(
        task_id = 'run_gold_notebook',
        input_nb = globant_pipeline_path+'load_gold_data.ipynb',
        output_nb = globant_pipeline_path+'load_gold_data.ipynb'
    )

    [run_bronze_notebook >> run_silver_notebook >> run_gold_notebook]

# Dockerfile para Apache Airflow
FROM apache/airflow:2.10.2

# Copia los archivos del DAG y otros scripts necesarios al contenedor
COPY ./globant_pipeline /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

# Establece el directorio de trabajo
WORKDIR /opt/airflow

# Instala las dependencias necesarias para Airflow
COPY ./requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Configura el entorno
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__PARALLELISM=10

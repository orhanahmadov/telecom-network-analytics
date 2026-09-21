# Airflow image with dbt installed in its OWN virtualenv.
# dbt and Airflow pin overlapping dependencies; keeping dbt in an isolated venv
# avoids resolver conflicts. DAG tasks call /home/airflow/dbt-venv/bin/dbt.
FROM apache/airflow:2.10.2-python3.11

USER airflow
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
         dbt-core==1.8.7 \
         dbt-postgres==1.8.2

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def etl(schema, table):
    cur = get_Redshift_connection()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    id int,
    created_at timestamp,
    score int    
    );
    
"""

    logging.info(drop_recreate_sql)
    try:
        cur.execute(drop_recreate_sql)
        cur.execute("Commit;")

    except Exception as e:
        cur.execute("Rollback;")
        raise
with DAG(
        dag_id = 'MySQL_Test',
        start_date = datetime(2022,5,5),
        catchup=False,
        tags=['example'],
        schedule = '0 2 * * *'
        ) as dag:


    etl("nalala8200","sql_test")

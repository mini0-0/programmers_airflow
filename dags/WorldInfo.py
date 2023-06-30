from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import logging
import requests

def get_Redshift_connection(autocommit=True): 
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract_transform():
    logging.info("Extract Start")
    # 데이터 url
    response = requests.get("https://restcountries.com/v3/all")
    countries = response.json()
    records = []

    for country in countries:
        name = country['name']['common']
        population = country['population']
        area = country['area']

        records.append([name, population, area])

    logging.info("Extract End")
    return records


@task
def load(schema, table, records):
        logging.info("load started")
        cur = get_Redshift_connection()
        try:
            cur.execute("BEGIN;")
            # 기존 테이블 존재하면 삭제
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
                country VARCHAR(256) primary key ,
                area FLOAT,
                population INT
                );""")

                    
            for r in records:
                sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]});"
                print(sql)
                cur.execute(sql)
               

            cur.execute("COMMIT;")   # cur.execute("END;")
        
        except Exception as error:
                print(error)
                cur.execute("ROLLBACK;")
                raise
        logging.info("load done")

with DAG(
    dag_id = 'WorldInfo',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    records = extract_transform()
    load("nalala8200", "worldinfo", records)



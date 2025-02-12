from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
import logging

from io import StringIO

dag = DAG(
    'fin_cotacoes_bcb_classic',
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'start_date': datetime(2023, 1, 1)
    },
    catchup=False,
    tags=["bcb"]
)

###### EXTRAÇÃO ######

def extract(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = base_url + ds_nodash + ".csv"
    logging.warning(full_url)

    try:
        response = requests.get(full_url)
        if response.status_code == 200:
            # Tentando decodificar para UTF-8
            csv_data = response.content.decode('utf-8')
            return csv_data
        else:
            logging.error(f"Erro ao acessar o arquivo: {response.status_code}")
    except Exception as e:
        logging.error(f"Erro ao realizar o request: {e}")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

###### TRANSFORMAÇÃO ######

def transform(**kwargs):
    cotacoes = kwargs['ti'].xcom_pull(task_ids='extract')
    csvStringIO = StringIO(cotacoes)

    column_names = [
        "DT_FECHAMENTO",
        "COD_MOEDA",
        "TIPO_MOEDA",
        "DESC_MOEDA",
        "TAXA_COMPRA",
        "TAXA_VENDA",
        "PARIDADE_COMPRA",
        "PARIDADE_VENDA"
    ]

    data_types = {
        "DT_FECHAMENTO": str,
        "COD_MOEDA": int,
        "TIPO_MOEDA": str,
        "DESC_MOEDA": str,
        "TAXA_COMPRA": float,
        "TAXA_VENDA": float,
        "PARIDADE_COMPRA": float,
        "PARIDADE_VENDA": float
    }

    parse_dates = ["DT_FECHAMENTO"]

    try:
        # Tentando ler o CSV com UTF-8
        df = pd.read_csv(
            csvStringIO,
            sep=";",
            decimal=",",
            thousands=".",
            encoding="utf-8",
            header=None,
            names=column_names,
            dtype=data_types,
            parse_dates=parse_dates
        )
    except UnicodeDecodeError:
        # Caso ocorra um erro de codificação, tentamos com ISO-8859-1
        logging.warning("Erro ao decodificar com UTF-8. Tentando com ISO-8859-1.")
        csvStringIO.seek(0)  # Reseta o cursor do StringIO
        df = pd.read_csv(
            csvStringIO,
            sep=";",
            decimal=",",
            thousands=".",
            encoding="ISO-8859-1",
            header=None,
            names=column_names,
            dtype=data_types,
            parse_dates=parse_dates
        )

    df['data_processamento'] = datetime.now()
    return df

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

#### CREATE TABLE ####
create_database_ddl = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'astro') THEN
            CREATE DATABASE astro;
        END IF;
    END
    $$;
"""

create_database_postgres = PostgresOperator(
    task_id='create_database_postgres',
    postgres_conn_id='postgres_default',
    sql=create_database_ddl,
    autocommit=True,
    dag=dag
)

create_schema_ddl = """
    CREATE SCHEMA IF NOT EXISTS astro;
"""

create_schema_postgres = PostgresOperator(
    task_id='create_schema_postgres',
    postgres_conn_id='postgres_astro',
    sql=create_schema_ddl,
    autocommit=True,  # Garantir que a transação seja desativada
    dag=dag
)


create_table_ddl = """
    CREATE TABLE IF NOT EXISTS astro.cotacoes (
        dt_fechamento DATE,
        cod_moeda TEXT,
        tipo_moeda TEXT,
        desc_moeda TEXT,
        taxa_compra REAL,
        taxa_venda REAL,
        paridade_compra REAL,
        paridade_venda real,
        data_processamento TIMESTAMP,
        CONSTRAINT table_pk PRIMARY KEY (dt_fechamento, cod_moeda)
    );
"""

create_table_postgres = PostgresOperator(
    task_id='create_table_postgres',
    postgres_conn_id='postgres_astro',
    sql=create_table_ddl,
    dag=dag
)

#### LOAD ####

def load(**kwargs):
    cotacoes_df = kwargs['ti'].xcom_pull(task_ids='transform')
    table_name = "astro.cotacoes"  # Certifique-se de incluir o esquema
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_astro')

    # Verifique se a tabela existe
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'astro'
            AND table_name = 'cotacoes'
        );
    """)
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        logging.error("Tabela 'cotacoes' não existe no esquema 'astro'.")
        return

    # Se a tabela existir, prossegue para inserir os dados
    rows = list(cotacoes_df.itertuples(index=False))
    
    postgres_hook.insert_rows(
        table=table_name,
        rows=rows,
        target_fields=["dt_fechamento", "cod_moeda", "tipo_moeda", "desc_moeda", 
                       "taxa_compra", "taxa_venda", "paridade_compra", "paridade_venda", 
                       "data_processamento"]
    )


load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# Definindo a ordem das tarefas
extract_task >> transform_task >> create_database_postgres >> create_schema_postgres>> create_table_postgres >> load_task

import yaml
import logging
import airflow
import pandas_gbq
import pandas as pd
from airflow import DAG
from google.cloud import bigquery
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


try:
    with open('./gcs/dags/demo/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    sql_path = config['sql_path']
    project_id = config['project_id']
    dataset = config['dataset']

    logging.info(f"SQL path: {sql_path}")
except Exception as e:
    logging.error(e)
    raise AirflowException(f"Failed to load configuration: {e}")


def read_and_transform():
    query1 = """
        SELECT *
        FROM `f{project_id}.astronomer_poc.stg_receipts`
    """

    query2 = """
        SELECT *
        FROM `f{project_id}.astronomer_poc.stg_transactions`
    """

    receipts_df = pd.read_gbq(query1, project_id=project_id, dialect='standard')
    transactions_df = pd.read_gbq(query2, project_id=project_id, dialect='standard')

    print(receipts_df.columns)
    print(transactions_df.columns)

    joined_df = pd.merge(receipts_df, transactions_df, on=['block_hash', 'transaction_hash'], how='left')

    print(joined_df.columns)

    transactions_per_address = joined_df.groupby('from_address_x').size().reset_index(name='transaction_count')

    transactions_per_address.rename(columns={
        'from_address_x': 'from_address'
    }, inplace=True)

    print(transactions_per_address.columns)

    transactions_per_address.to_gbq(
        destination_table=f"{dataset}.transactions_per_address",
        project_id=project_id,
        if_exists="replace"
    )


def transformlogData():
    client = bigquery.Client()
    project = project_id
    dataset_id = dataset
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table("stg_logs")
    table = client.get_table(table_ref)

    df = client.list_rows(table).to_dataframe()

    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])

    df['date'] = df['block_timestamp'].dt.date.astype(str)
    df['time'] = df['block_timestamp'].dt.time.astype(str)
    df['topics']=df['topics'].astype(str)

    df.drop(columns=['block_timestamp'], inplace=True)
    
    return df

def loadDataToBigQuery(**context):
    dataframe = context['transformed_data']

    # print(dataframe)

    dataframe.to_gbq(
        destination_table=f"{project_id}.astronomer_poc.cur_logs", 
        project_id=project_id, 
        if_exists='replace'
    )

    return "DONE!"

default_args = {
    'owner': 'Airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=50)
}
   
dag = DAG(
    dag_id="astronomer_etl",
    default_args=default_args,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    template_searchpath=[sql_path],
    tags=['ingestion', 'transformation', 'astronomer']
)

start_pipeline = DummyOperator(
    task_id="start_pipeline",
    dag=dag
)

ingest_data = BigQueryInsertJobOperator(
    task_id='ingest_data',
    configuration={
        'query': {
            'query': "{% include 'ingest.sql' %}",
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag,
)

transformation_1 = BigQueryInsertJobOperator(
    task_id='transformation_1',
    configuration={
        'query': {
            'query': "{% include 'gas_utilization.sql' %}",
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag,
)

transformation_2 = PythonOperator(
    task_id="transformation_2",
    python_callable=read_and_transform,
    provide_context=True,
    dag=dag
)

transformation_3 = BigQueryInsertJobOperator(
    task_id='transformation_3',
    configuration={
        'query': {
            'query': "{% include 'transform_receipts.sql' %}",
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag,
)

transformation_4 = PythonOperator(
    task_id="transformation_4",
    python_callable=transformlogData,
    provide_context=True,
    dag=dag
)

load_to_bq = PythonOperator(
    task_id="load_to_bq",
    python_callable=loadDataToBigQuery,
    provide_context=True,
    op_kwargs={
        'transformed_data': transformation_4.output
    },
    dag=dag
)

end_pipeline = DummyOperator(
    task_id="end_pipeline",
    dag=dag
)

start_pipeline >> ingest_data >> transformation_1 >> transformation_2 >> transformation_3 >> transformation_4 >> load_to_bq >> end_pipeline
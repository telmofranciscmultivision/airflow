from datetime import timedelta  

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.azure.operators.azure_cosmos import AzureCosmosInsertDocumentOperator

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='cosmosdb_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)

t2 = AzureCosmosInsertDocumentOperator(
     task_id='insert_cosmos_file',
     database_name='test',
     collection_name='new-collection',
     document={"id": "someuniqueid", "param1": "value1", "param2": "value2"},
     azure_cosmos_conn_id='azure_conn',
     dag = dag,
)

#run_this_last = AzureCosmosInsertDocumentOperator(
#    task_id='prestoToMysqlOperator',
#    database_name='test',
#    collection_name ='products',
#    document = '{}',
#    azure_cosmos_conn_id ='azure_conn',
#    dag=dag,
#)


t2

if __name__ == "__main__":
    dag.cli()

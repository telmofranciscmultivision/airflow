from datetime import timedelta, date, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.transfers.presto_to_mysql import PrestoToMySqlOperator
from airflow.providers.mysql.transfers.trino_to_mysql import TrinoToMySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import time
args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='use_case_copy',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)

 
drop_table_a = MySqlOperator(task_id='drop_table_a', mysql_conn_id='mysql_conn', sql="DROP TABLE A;", dag=dag)

drop_table_b = MySqlOperator(task_id='drop_table_b', mysql_conn_id='mysql_conn', sql="DROP TABLE B;", dag=dag)

create_table_a = MySqlOperator(task_id='create_table_a', mysql_conn_id='mysql_conn', sql="CREATE TABLE IF NOT EXISTS movies_db.A(test VARCHAR(255) NOT NULL, date DATETIME)", dag=dag)

create_table_b = MySqlOperator(task_id='create_table_b', mysql_conn_id='mysql_conn', sql="CREATE TABLE IF NOT EXISTS movies_db.B(test VARCHAR(255) NOT NULL, date DATETIME)", dag=dag)


insert_values_a_1 = MySqlOperator(task_id='insert_values_a_1', mysql_conn_id='mysql_conn', sql="INSERT INTO movies_db.A (`test`,`date`) VALUES ('lalal',now());", dag=dag)
insert_values_a_2 = MySqlOperator(task_id='insert_values_a_2', mysql_conn_id='mysql_conn', sql="INSERT INTO movies_db.A (`test`,`date`) VALUES ('lala2',now());", dag=dag)
insert_values_a_3 = MySqlOperator(task_id='insert_values_a_3', mysql_conn_id='mysql_conn', sql="INSERT INTO movies_db.A (`test`,`date`) VALUES ('lala3',now());", dag=dag)


def get_max_value(**kwargs):
    mysqlserver = MySqlHook('mysql_conn')
    query = "SELECT MAX(date) FROM movies_db.A;"
    raw_data = mysqlserver.get_records(query)
    print(raw_data)
    print(raw_data[0][0])
    kwargs['ti'].xcom_push(key='max date', value=raw_data[0][0])
    print(raw_data[0][0])

def get_max_value_copy(**kwargs):
    mysqlserver = PostgresHook('postgres_conn_airflow')
    query = "SELECT MAX(date) FROM public.b;"
    raw_data = mysqlserver.get_records(query)
    print(raw_data)
    print(raw_data[0][0])
    kwargs['ti'].xcom_push(key='max date postgres', value=raw_data[0][0])
    print(raw_data[0][0])
    
def delete_b_bigger_than_max(**kwargs):
    mysqlserver = MySqlHook('mysql_conn')
    maxVal= kwargs['ti'].xcom_pull(key=None,task_ids='getMaxValueCopy')
    query = "DELETE FROM movies_db.B WHERE date > '" + str(maxVal) + "';"
    print(query)
    raw_data = mysqlserver.get_records(query)
    
def insert_b_from_a_where_bigger_than_max(**kwargs):
    mysqlserver = MySqlHook('mysql_conn')
    maxVal= kwargs['ti'].xcom_pull(key=None,task_ids='getMaxValueCopy')     
    query_2="(select * from movies_db.A WHERE (movies_db.A.date > '" + str(maxVal) + "'));"    
    query = "INSERT INTO movies_db.B " + query_2
    insert = MySqlOperator(task_id='insert_max', mysql_conn_id='mysql_conn', sql=query)
    insert.execute(context=kwargs)


 
getMaxValue = PythonOperator(
    task_id='getMaxValue',
    provide_context=True,
    python_callable=get_max_value,
    dag=dag)
    
deleteBBiggerThanMax = PythonOperator(
    task_id='deleteBBiggerThanMax',
    provide_context=True,
    python_callable=delete_b_bigger_than_max,
    dag=dag)
    
insert_values_a_4 = MySqlOperator(task_id='insert_values_a_4', mysql_conn_id='mysql_conn', sql="INSERT INTO A (`test`,`date`) VALUES ('lala4',now());", dag=dag)
    
insertBFromAWhereBiggerThanMax = PythonOperator(
    task_id='insertBFromAWhereBiggerThanMax',
    provide_context=True,
    python_callable=insert_b_from_a_where_bigger_than_max,
    dag=dag)

getMaxValueCopy = PythonOperator(
    task_id='getMaxValueCopy',
    provide_context=True,
    python_callable=get_max_value_copy,
    dag=dag)    
    
getMaxValueCopy >> drop_table_a >> drop_table_b >> create_table_a >> create_table_b >> insert_values_a_1 >> insert_values_a_2 >> insert_values_a_3 >> getMaxValue >> insert_values_a_4 >> deleteBBiggerThanMax >> insertBFromAWhereBiggerThanMax


if __name__ == "__main__":
    dag.cli()
    
    

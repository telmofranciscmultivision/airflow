from datetime import timedelta  

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.transfers.presto_to_mysql import PrestoToMySqlOperator
from airflow.providers.mysql.transfers.trino_to_mysql import TrinoToMySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook


args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='data_transfer_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)


def test_task_func():
    mysqlserver = MySqlHook('mysql_conn')
    query = "select count(*) from movies_db.persons"
    raw_data = mysqlserver.get_records(query)
    print('TEST TASK:')
    print(raw_data[0][0])

run_this_first = TrinoToMySqlOperator(
    task_id='create_copy_table',
    sql='select * from mysql.movies_db.persons;',
    mysql_table='copy_persons',
    mysql_conn_id='mysql_conn',
    trino_conn_id='presto_conn',
    mysql_preoperator='TRUNCATE copy_persons',
    dag=dag,
)


#run_this_last = TrinoToMySqlOperator(
#    task_id='prestoToMysqlOperator',
#    sql='select * from mysql.movies_db.persons where mysql.movies_db.persons.personId not in (select mysql.movies_db.copy_table_trino.personId from mysql.movies_db.copy_table_trino);',
#    mysql_table='copy_table_trino',
#    mysql_conn_id='mysql_conn',
#    trino_conn_id='presto_conn',
    #mysql_preoperator='TRUNCATE TABLE',
#    dag=dag,
#)

test_task = PythonOperator(
    task_id='print',
    provide_context=True,
    python_callable=test_task_func,
    dag=dag)

run_this_first >>  test_task

if __name__ == "__main__":
    dag.cli()

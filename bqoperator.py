from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from airflow.models import DAG
from datetime import datetime,timedelta, date
import sys
from airflow.models import Variable

default_args={
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2021, 11, 25),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=1)
}

dag = DAG('BQoperator',
          default_args=default_args,
          schedule_interval='5 16 * * *', # in UTC
          catchup=False,
          max_active_runs=1)

START_TASK = DummyOperator(task_id="START",
                           dag=dag)
                           
project='`dmgcp-training-q4-2021.'
dataset='DB_FRESHERS_02.'
table='cust_bal_sourav`'

QUERY = bigquery_operator.BigQueryOperator(
        task_id='QUERY',
        bigquery_conn_id='bigquery_default',
        sql="""SELECT * FROM `"""+project+dataset+table
#sql="""SELECT * FROM `dmgcp-training-q4-2021.DB_FRESHERS_02.cust_bal_sourav""",
        use_legacy_sql=False,
        dag=dag)

END_TASK = DummyOperator(task_id="END",
                        dag=dag)

START_TASK.set_downstream(QUERY)
QUERY.set_downstream(END_TASK)

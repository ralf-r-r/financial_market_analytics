from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (FinanceDataToS3Operator, StageToRedshiftOperator)

# - define start date, end date and schedule interval
default_args = {
    'owner': 'ralf',
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2020, 7, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': True,
    'depends_on_past': False
}

dag = DAG('01_finance_dag',
          default_args=default_args,
          description='Load finance data from yahoo finance and quandl and insert data into redshiift tables',
          schedule_interval='0 0 1 */3 *'
          )

# - define start_operator
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# - define staging operators


finance_api_data_to_s3 = FinanceDataToS3Operator(
    task_id='load_finace_data_to_s3',
    dag = dag,
    aws_credentials_id='aws_credentials',
    s3_bucket='rrrfinance',
    s3_region='eu-central-1',
    date='{{ ds }}',
    provide_context=True
)

stage_tickers_to_redshift = StageToRedshiftOperator(
     task_id='stage_tickers',
     dag=dag,
     aws_credentials_id='aws_credentials',
     rs_conn_id='redshift',
     rs_target_table='public.staging_tickers',
     s3_bucket='s3://rrrfinance/',
     s3_key="tickers/year-quarter-tickers.csv",
     s3_region='eu-central-1',
     date='{{ ds }}',
     provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# - define task dependencies
start_operator >> finance_api_data_to_s3
finance_api_data_to_s3 >> stage_tickers_to_redshift
stage_tickers_to_redshift >> end_operator

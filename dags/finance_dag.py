from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.finance_data_s3 import FinanceDataToS3Operator

# - define start date, end date and schedule interval
default_args = {
    'owner': 'ralf',
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2021, 1, 1),
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
# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag,
#     aws_credentials_id='aws_credentials',
#     rs_conn_id='redshift',
#     rs_target_table='public.staging_events',
#     s3_bucket='s3://udacity-dend/',
#     s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
#     s3_jsonpath='s3://udacity-dend/log_json_path.json',
#     s3_region='us-west-2',
#     provide_context=True
# )

finance_api_data_to_s3 = FinanceDataToS3Operator(
    task_id='load_data_to_s3',
    dag = dag,
    aws_credentials_id='aws_credentials',
    s3_bucket='rrrfinance',
    s3_region='eu-central-1',
    date='{{ ds }}',
    provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# - define task dependencies
start_operator >> finance_api_data_to_s3
finance_api_data_to_s3 >> end_operator

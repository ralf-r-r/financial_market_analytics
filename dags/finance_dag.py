from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (FinanceDataToS3Operator, StageToRedshiftOperator, LoadRedShiftOperator, DataQualityOperator)
from helpers import SqlQueries, create_sql_checks

# - define start date, end date and schedule interval
default_args = {
    'owner': 'ralf',
    'start_date': datetime(2000, 1, 1),
    'end_date': datetime(2021, 12, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': True,
    'depends_on_past': False
}

dag = DAG('01_finance_dag',
          default_args=default_args,
          description='Load finance data from yahoo finance and quandl and insert data into redshift tables',
          schedule_interval='0 0 1 */3 *'  # run the dag once every quarter on the first day of the month
          )

# - define start_operator
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# - define operators to download data from API to S3
finance_api_data_to_s3 = FinanceDataToS3Operator(
    task_id='load_finace_data_to_s3',
    dag=dag,
    aws_credentials_id='aws_credentials',
    s3_bucket='rrrfinance',
    s3_region='eu-central-1',
    date='{{ ds }}',
    provide_context=True
)

# - define staging operators
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
    provide_context=True,
    use_date=True
)

stage_spot_prices_to_redshift = StageToRedshiftOperator(
    task_id='stage_spot_prices',
    dag=dag,
    aws_credentials_id='aws_credentials',
    rs_conn_id='redshift',
    rs_target_table='public.staging_spot_prices',
    s3_bucket='s3://rrrfinance/',
    s3_key="spot_prices/year-quarter-spot_prices.csv",
    s3_region='eu-central-1',
    date='{{ ds }}',
    provide_context=True,
    use_date=True
)

stage_fundamentals_to_redshift = StageToRedshiftOperator(
    task_id='stage_fundamentals',
    dag=dag,
    aws_credentials_id='aws_credentials',
    rs_conn_id='redshift',
    rs_target_table='public.staging_fundamentals',
    s3_bucket='s3://rrrfinance/',
    s3_key="fundamentals/SHARADAR_SF1.csv",
    s3_region='eu-central-1',
    date='{{ ds }}',
    provide_context=True,
    use_date=False
)

stage_money_supply_to_redshift = StageToRedshiftOperator(
    task_id='stage_money_supply',
    dag=dag,
    aws_credentials_id='aws_credentials',
    rs_conn_id='redshift',
    rs_target_table='public.staging_m3_money_supply',
    s3_bucket='s3://rrrfinance/',
    s3_key="m3_money_supply/m3_money_supply.csv",
    s3_region='eu-central-1',
    date='{{ ds }}',
    provide_context=True,
    use_date=False
)

# - define fact and dimension operators
load_spot_prices = LoadRedShiftOperator(
    task_id='Load_spot_prices',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=False,
    rs_table_name='spot_prices',
    sql_insert=SqlQueries.spot_prices_insert,
    provide_context=True
)

load_fundamentals = LoadRedShiftOperator(
    task_id='Load_fundamentals',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=False,
    rs_table_name='fundamentals',
    sql_insert=SqlQueries.fundamentals_insert,
    provide_context=True
)

load_tickers = LoadRedShiftOperator(
    task_id='Load_tickers',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=False,
    rs_table_name='ticker',
    sql_insert=SqlQueries.tickers_insert,
    provide_context=True
)

load_m3_money_supply = LoadRedShiftOperator(
    task_id='Load_m3_money_supply',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=False,
    rs_table_name='m3_money_supply',
    sql_insert=SqlQueries.m3_money_supply_insert,
    provide_context=True
)

# - define data quality checks
checks = create_sql_checks()
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    rs_conn_id='redshift',
    data_quality_checks=checks
)

# - define end operator
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# - define task dependencies
start_operator >> finance_api_data_to_s3
finance_api_data_to_s3 >> [stage_tickers_to_redshift,
                           stage_spot_prices_to_redshift,
                           stage_fundamentals_to_redshift,
                           stage_money_supply_to_redshift]

[stage_tickers_to_redshift,
 stage_spot_prices_to_redshift,
 stage_fundamentals_to_redshift,
 stage_money_supply_to_redshift] >> load_spot_prices

load_spot_prices >> [load_fundamentals,
                     load_tickers,
                     load_m3_money_supply]

[load_fundamentals,
 load_tickers,
 load_m3_money_supply] >> run_quality_checks

run_quality_checks >> end_operator

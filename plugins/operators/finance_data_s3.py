from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from yahoo_fin import stock_info as si
import pandas as pd
import boto3
from helpers.tickers import df_constant_tickers
from helpers.utils import upload_df_to_S3
from typing import List
import yfinance as yf
import datetime
from datetime import datetime, timedelta

def subtract_months_from_date(dt, n):
    date_object = dt

    for k in range(0, n):
        date_object = date_object.replace(day=1) - timedelta(1)
        date_object = date_object.replace(day=1)

    return date_object


def add_market_index(df_input, index_name)->pd.DataFrame:
    df = df_input.copy()
    df["market_index"] = index_name
    return df


def get_tickers() -> pd.DataFrame:
    # get raw data from api
    df_list = []
    df1 = si.tickers_nasdaq(True)
    df_list.append(add_market_index(df1, "nasdaq"))
    df2 = si.tickers_ftse250(True)
    df_list.append(add_market_index(df2, "ftse250"))
    si.tickers_dow(True)
    df_list.append(add_market_index(df2, "dow"))
    si.tickers_sp500(True)
    df_list.append(add_market_index(df2, "sp500"))
    df_tickers = pd.concat(df_list)

    # process the raw data
    df_tickers = df_tickers.fillna('')
    df_tickers["ticker"] = df_tickers["Symbol"] + df_tickers["Ticker"]
    df_tickers["name"] = df_tickers["Security Name"] + df_tickers["Company"]
    df_tickers = df_tickers[["ticker", "name", "market_index"]]
    df_tickers = df_tickers[df_tickers["ticker"] != "  "]
    df_tickers = df_tickers[df_tickers["ticker"] != " "]
    df_tickers = df_tickers[df_tickers["ticker"] != ""]

    # combine the api data with hardcoded tickers
    df = pd.concat([df_constant_tickers, df_tickers])
    return df


def get_all_stocks_data(tickers: List[str], start_date, end_date)->pd.DataFrame:
    N = len(tickers)
    iterations = 4
    n = int(N / iterations)

    df_list = []
    for k in range(0, iterations + 1):
        print(k, "of", iterations + 1)
        tickers_tmp = tickers[k:k + n]
        if len(tickers_tmp) > 0:
            sep = " "
            tickers_string = sep.join(tickers_tmp)
            df_tmp = yf.download(tickers_string, start=start_date, end=end_date, group_by='ticker', interval="1d")
            df_list.append(df_tmp)

    df_time_series = pd.concat(df_list)

    df_list = []
    for ticker in list(df_time_series.columns.levels[0]):
        df_tmp = df_time_series[ticker]
        df_tmp["ticker"] = ticker
        df_tmp = df_tmp[["ticker", "Close", "Volume"]]
        df_list.append(df_tmp)

    df_ts = pd.concat(df_list)
    df_ts = df_ts.reset_index()
    df_ts["Date"] = df_ts["Date"].astype(str)
    df_ts["year"] = df_ts["Date"].str.split("-", expand=True)[0].astype(int)
    df_ts["month"] = df_ts["Date"].str.split("-", expand=True)[1].astype(int)
    df_ts["day"] = df_ts["Date"].str.split("-", expand=True)[2].astype(int)

    df_ts = df_ts.dropna()

    renamed_columns = {
        "Date": "date",
        "Close": "close",
        "Volume": "volume"
    }

    df_ts = df_ts.rename(columns=renamed_columns)
    return df_ts


class FinanceDataToS3Operator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('date',)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id: str,
                 s3_bucket: str,
                 s3_region: str,
                 start_date: str,
                 *args,
                 **kwargs):
        """
        :param aws_credentials_id: str, aws credentials id
        :param s3_bucket: str, name of the aws s3 bucket
        :param s3_key: str, name of the s3 bucket key
        :param s3_region: str, the region of the s3 bucket
        """
        super(FinanceDataToS3Operator, self).__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.start_date = start_date

    def execute(self, context):
        start_date = self.start_date.format(**context)
        year, month, day = start_date.split("-")
        start_date_object = datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")
        end_date_object = subtract_months_from_date(start_date_object, 3)
        end_date = end_date_object.strftime("%Y-%m-%d")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # get ticker data from yfinance and upload it to s3
        self.log.info('starting to get stock tickers from from api with yfinance')
        df_tickers = get_tickers()

        self.log.info('starting to upload stock tickers data to s3')
        filename_tickers = + year + "-" + month + "-tickers.csv"
        s3_key_tickers = "/tickers/" + year + "/" + filename_tickers
        upload_df_to_S3(df=df_tickers,
                        aws_access_key_id=credentials.access_key,
                        aws_secret_access_key=credentials.secret_key,
                        region=self.s3_region,
                        bucket=self.s3_bucket,
                        key=s3_key_tickers)

        self.log.info('uploading tickers data to s3 successful!')

        # get stock price and volume data from yfinance and upload it to s3
        self.log.info('starting to get stock price and volume data from from api with yfinance')
        df_price_volume = get_all_stocks_data(tickers=list(df_tickers["ticker"].unique()),
                                              start_date=start_date,
                                              end_date=end_date)

        self.log.info('starting to upload stock price and volume data to s3')

        filename_tickers = + year + "-" + month + "-stock_prices_volumes.csv"
        s3_key_tickers = "/stock_prices_volumes/" + year + "/" + filename_tickers
        upload_df_to_S3(df=df_price_volume,
                        aws_access_key_id=credentials.access_key,
                        aws_secret_access_key=credentials.secret_key,
                        region=self.s3_region,
                        bucket=self.s3_bucket,
                        key=s3_key_tickers)

        self.log.info('uploading stock price and volume data to s3 successful!')

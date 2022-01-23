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
import re
from pytickersymbols import PyTickerSymbols

def subtract_months_from_date(dt, n):
    date_object = dt

    for k in range(0, n):
        date_object = date_object.replace(day=1) - timedelta(1)
        date_object = date_object.replace(day=1)

    return date_object


def get_tickers() -> pd.DataFrame:
    """
    downloads stock ticker symbols from pytickersymbols
    :return: pd.DataFrame, ticker symbols
    """
    # Download ticker symbols
    stock_ticker_symbols = PyTickerSymbols()

    smi_tickers = list(stock_ticker_symbols.get_stocks_by_index('SMI'))
    dfp_tickers_smi = pd.DataFrame(smi_tickers)

    sp500_tickers = list(stock_ticker_symbols.get_stocks_by_index('S&P 500'))
    dfp_tickers_sp500 = pd.DataFrame(sp500_tickers)

    nasdaq100_tickers = list(stock_ticker_symbols.get_stocks_by_index('NASDAQ 100'))
    dfp_tickers_nasdaq100 = pd.DataFrame(nasdaq100_tickers)

    dfp_tickers = pd.concat([dfp_tickers_smi,
                             dfp_tickers_sp500,
                             dfp_tickers_nasdaq100])

    # clean the ticker symbols
    dfp_tickers = dfp_tickers[dfp_tickers["symbol"].isnull() == False]
    dfp_tickers = dfp_tickers.fillna('')
    dfp_tickers["symbol"] = dfp_tickers["symbol"].apply(lambda x: re.sub('\s{2,}', ' ', x))
    dfp_tickers = dfp_tickers[dfp_tickers["symbol"] != " "]
    dfp_tickers = dfp_tickers[dfp_tickers["symbol"].str.len() > 0]
    dfp_tickers["indices"] = dfp_tickers["indices"].apply(lambda x: ",".join(x))

    dfp_tickers = dfp_tickers.drop_duplicates(["symbol"])
    cols = [
        "symbol",
        "name",
        "country",
        "indices"
    ]
    dfp_tickers = dfp_tickers[cols]

    # combine the data from pytickers with hardcoded tickers
    df = pd.concat([df_constant_tickers, dfp_tickers])
    return df


def get_all_stocks_data(tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Downloads price and volume data from yahoo finance for a list of ticker symbols for a time
    period specified by a start date and end date
    :param tickers: List[str], ticker symbols for which data is downloaded form yahoo finance
    :param start_date: str, format: %Y-%m-%d, the start date
    :param end_date: str, format: %Y-%m-%d, the end date
    :return: pd.DataFrame, spot price data
    """
    n_tickers = len(tickers)
    iterations = 2
    step_size = int(n_tickers / iterations-1)

    df_list = []
    for k in range(0, iterations):
        print(k, "of", iterations)
        tickers_tmp = tickers[k:k + step_size]
        if len(tickers_tmp) > 0:
            sep = " "
            tickers_string = sep.join(tickers_tmp)
            df_tmp = yf.download(tickers_string,
                                 start=start_date,
                                 end=end_date,
                                 group_by='ticker',
                                 interval="1d")
            df_list.append(df_tmp)

    df_time_series = pd.concat(df_list)

    df_list = []
    for ticker in list(df_time_series.columns.levels[0]):
        df_tmp = df_time_series[ticker]
        df_tmp["symbol"] = ticker
        df_tmp = df_tmp[["symbol", "Close", "Volume"]]
        df_list.append(df_tmp)

    df_ts = pd.concat(df_list)
    df_ts = df_ts.reset_index()
    df_ts["Date"] = df_ts["Date"].astype(str)
    df_ts["year"] = df_ts["Date"].str.split("-", expand=True)[0].astype(int)
    df_ts["month"] = df_ts["Date"].str.split("-", expand=True)[1].astype(int)
    df_ts["day"] = df_ts["Date"].str.split("-", expand=True)[2].astype(int)
    df_ts = df_ts.drop(columns= ["Date"])
    df_ts = df_ts.dropna()

    renamed_columns = {
        "Close": "closing_price",
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
                 end_date: str,
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
        self.end_date = end_date

    def execute(self, context):
        # convert date strings
        end_date = self.end_date.format(**context)
        end_date_object = datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")
        start_date_object = subtract_months_from_date(end_date_object, 3)
        start_date = start_date_object.strftime("%Y-%m-%d")
        quarter = start_date_object.quarter
        year, month, day = start_date.split("-")

        # get credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # get ticker data from pytickersymbols and upload data to s3
        self.log.info('starting to get stock ticker symbols from pytickersymbols')
        dfp_tickers = get_tickers()

        self.log.info('starting to upload stock tickers data to s3')
        s3_key_tickers = "/tickers/" + year + "_" + quarter + "_tickers.csv"
        upload_df_to_S3(df=dfp_tickers,
                        aws_access_key_id=credentials.access_key,
                        aws_secret_access_key=credentials.secret_key,
                        region=self.s3_region,
                        bucket=self.s3_bucket,
                        key=s3_key_tickers)

        self.log.info('uploading tickers data to s3 successful!')

        # get stock price and volume data from yfinance and upload it to s3
        self.log.info('starting to get stock price and volume data from from api with yfinance')
        df_price_volume = get_all_stocks_data(tickers=list(dfp_tickers["symbol"].unique()),
                                              start_date=start_date,
                                              end_date=end_date)

        self.log.info('starting to upload stock price and volume data to s3')

        s3_key_tickers = "/spot_prices/" + year + "_" + quarter + "_" + "spot_prices.csv"
        upload_df_to_S3(df=df_price_volume,
                        aws_access_key_id=credentials.access_key,
                        aws_secret_access_key=credentials.secret_key,
                        region=self.s3_region,
                        bucket=self.s3_bucket,
                        key=s3_key_tickers)

        self.log.info('uploading stock price and volume data to s3 successful!')

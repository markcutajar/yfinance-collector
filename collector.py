import os
import re
import tempfile

from argparse import ArgumentParser
from datetime import datetime, date, timedelta

import pandas as pd
import yfinance as yf

from google.cloud import storage


class YFinanceCollector:

    name = 'yfinance'

    columns = [
        'datetime', 'open', 'high', 'low',
        'close', 'volume', 'dividends',
        'stock_splits'
    ]

    def __init__(self, collecting_interval='1m'):
        self.interval = collecting_interval.lower()

    def collect(self, ticker: str, date: date) -> pd.DataFrame:
        
        start_date = date.strftime('%Y-%m-%d')
        end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')

        data = yf.Ticker(ticker)
        prices = data.history(start=start_date, end=end_date, interval=self.interval)
        prices = prices.reset_index()
        prices.columns = self.columns
        return prices


class Resampler:
    
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'dividends': 'sum',
        'stock_splits': 'sum'
    }

    @staticmethod
    def format_interval(interval: str) -> str:
        if re.match('^\dM$', interval):
            return interval.replace('M', 'T')
        return interval

    @classmethod
    def resample(cls, data: pd.DataFrame, interval: str) -> pd.DataFrame:
        interval = cls.format_interval(interval)
        df = data.set_index('datetime')
        df = df.resample(interval).agg(cls.agg_dict)
        df = df.reset_index()
        return df


class GCSManager:

    def __init__(self, bucket_name: str):
        
        self.bucket_name = bucket_name
        self.storage_client = storage.Client.from_service_account_json(
            'service-key-dinero.json'
        )
        self.bucket = self.storage_client.bucket(bucket_name)
        
    def upload(self, data, destination_blob_name):
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_file(data, content_type='text/csv')



class Collector:

    date_format = '%Y%m%d'
    filename_format ='{date}.csv'

    collect_interval = '1M'
    intervals = ['1M', '5M', '1H', '1D']

    def __init__(
        self, tickers: list, 
        bucket: str, path: str
    ):
        
        self.tickers = tickers
        self.upload_path = path
        self.resampler = Resampler()
        self.data_collector = YFinanceCollector(self.collect_interval)
        self.data_uploader = GCSManager(bucket)

    def collect(self, date: date):
        for ticker in self.tickers:
              
            data = self.data_collector.collect(ticker, date)

            for interval in self.intervals:

                if interval != self.collect_interval:
                    data = self.resampler.resample(data, interval)

                self.save_ticker_data(data, date, ticker, interval)
                

    def save_ticker_data(self, data, date, ticker, interval):

        filename = self.filename_format.format(
            date=date.strftime(self.date_format),
        )

        save_path = f'{self.upload_path}/{ticker}/{interval}/{filename}'
        
        print('Saving', date, ticker, interval, save_path, data.shape[0])
        self.save(data, save_path)

    def collect_many(self, dates: list):
        for date in dates:
            self.collect(date)

    def save(self, data, path):

        with tempfile.NamedTemporaryFile(delete=False) as temp:
            data.to_csv(temp.name, index=False)
            self.data_uploader.upload(temp, path)
            temp.close()
            os.remove(temp.name)


if __name__ == '__main__':

    parser = ArgumentParser(description='Collector for stock tickers')
    parser.add_argument(
        '--tickers', '-t',
        dest='tickers',
        required=True,
        type=str,
        help='Tickers to be collected in space seperated list form. Ex: AMZN AAPL MSFT'
    )
    parser.add_argument(
        '--bucket', '-b',
        dest='bucket',
        required=True,
        type=str,
        help='Bucket to save data in Google Cloud Storage'
    )
    parser.add_argument(
        '--path', '-p',
        dest='path',
        default='dinero-collector/yfinance',
        type=str,
        help='Path in bucket where to save data'
    )


    args = parser.parse_args()

    tickers = args.tickers.split(' ')

    collector = Collector(tickers, args.bucket, args.path)
    collector.collect(datetime.now().date() - timedelta(days=1))
""" Create some data to be used for testing the XetraETL pipeline. """

from datetime import datetime, timedelta

import pandas as pd

from xetra.common.meta_process import MetaProcessFormat
from xetra.transformers.xetra_transformer import XetraTargetConfig, XetraSourceConfig


class DataCreator:
    def _get_dates(self, length=6):
        """ Return a list of dates from today to today - length """
        return [
            (datetime.today().date() - timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            for x in range(6)
        ]
    
    def __init__(self):
        self.dates = self._get_dates()
        source_column_names = [
            'ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'
        ]
        report_column_names = [
            'ISIN', 'Date', 'opening_price_eur', 'closing_price_eur', 'minimum_price_eur',
            'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%'
        ]
        data = [
            ['AT0000A0E9W5', 'SANT', self.dates[5], '12:00', 20.19, 18.45, 18.20, 20.33, 877],
            ['AT0000A0E9W5', 'SANT', self.dates[4], '15:00', 18.27, 21.19, 18.27, 21.34, 987],
            ['AT0000A0E9W5', 'SANT', self.dates[3], '13:00', 20.21, 18.27, 18.21, 20.42, 633],
            ['AT0000A0E9W5', 'SANT', self.dates[3], '14:00', 18.27, 21.19, 18.27, 21.34, 455],
            ['AT0000A0E9W5', 'SANT', self.dates[2], '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
            ['AT0000A0E9W5', 'SANT', self.dates[2], '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
            ['AT0000A0E9W5', 'SANT', self.dates[1], '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
            ['AT0000A0E9W5', 'SANT', self.dates[1], '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
            ['AT0000A0E9W5', 'SANT', self.dates[1], '09:00', 24.22, 22.21, 22.21, 25.01, 1523]
        ]
        self.df_src = pd.DataFrame(data, columns=source_column_names)
       
        data_report = [
            ['AT0000A0E9W5', self.dates[3], 20.21, 21.19, 18.21, 21.34, 1088, 0.0],
            ['AT0000A0E9W5', self.dates[2], 20.58, 21.14, 18.89, 21.14, 10286, -0.24],
            ['AT0000A0E9W5', self.dates[1], 23.58, 22.21, 22.21, 25.01, 3586, 5.06]
        ]
        self.df_report = pd.DataFrame(data_report, columns=report_column_names)
        
    def populate_date_to_s3(self, s3_bucket_conn):
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[0:0],
            f'{self.dates[5]}/{self.dates[5]}_BINS_XETR12.csv',
            'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[1:1],
            f'{self.dates[4]}/{self.dates[4]}_BINS_XETR15.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[2:2],
            f'{self.dates[3]}/{self.dates[3]}_BINS_XETR13.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[3:3],
            f'{self.dates[3]}/{self.dates[3]}_BINS_XETR14.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[4:4],
            f'{self.dates[2]}/{self.dates[2]}_BINS_XETR07.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[5:5],
            f'{self.dates[2]}/{self.dates[2]}_BINS_XETR08.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[6:6],
            f'{self.dates[1]}/{self.dates[1]}_BINS_XETR07.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[7:7],
            f'{self.dates[1]}/{self.dates[1]}_BINS_XETR08.csv', 'csv'
        )
        s3_bucket_conn.write_df_to_s3(
            self.df_src.loc[8:8],
            f'{self.dates[1]}/{self.dates[1]}_BINS_XETR09.csv', 'csv'
        )

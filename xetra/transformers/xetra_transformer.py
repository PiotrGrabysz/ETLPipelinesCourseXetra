"""
Xetra ETL Component.
"""

from datetime import datetime
import logging
from typing import NamedTuple

import pandas as pd

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data.

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for ISIN in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_end_price: column name for ending price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_end_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data.

    col_isin: column name for ISIN in target
    col_date:  column name for ISIN in target
    col_opening_price:  column name for opening price in target
    col_closing_price:  column name for closing price in target
    col_min_price:  column name for minimum price in target
    col_max_price:  column name for maximum price in target
    col_daily_traded_volume:  column name for daily traded volume in target
    col_change:  column name for change to previous day's closing price in target
    key: basic key of target file
    key_date_format: date format of target file key
    format: file format of the target file
    """
    col_isin: str
    col_date: str
    col_opening_price: str
    col_closing_price: str
    col_min_price: str
    col_max_price: str
    col_daily_traded_volume: str
    col_change: str
    key: str
    key_date_format: str
    format: str


class XetraETL:
    """
    Reads the Xetra data, transforms and writes the transformed to target.
    """

    def __init__(
            self,
            s3_bucket_src: S3BucketConnector,
            s3_bucket_trg: S3BucketConnector,
            meta_key: str,
            src_args: XetraSourceConfig,
            trg_args: XetraTargetConfig
    ):
        """
        Constructor for XetraTransformer.

        :param s3_bucket_src: connection to a source S3 bucket
        :param s3_bucket_trg: connection to a target S3 bucket
        :param meta_key: used as self.meta_key -> key of the meta file
        :param src_args: NamedTuple class with source configuration data
        :param trg_args: NamedTuple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            s3_bucket_meta=self.s3_bucket_trg,
            first_date=self.src_args.src_first_extract_date,
            meta_key=self.meta_key
        )
        self.meta_update_list = [
            date for date in self.extract_date_list
            if date >= self.extract_date
        ]

    def extract(self):
        """
        Read the source data and concatenate it to pandas DataFrame.

        :returns:
            df: pandas DataFrame with extracted data.
        """
        self._logger.info('Extracting Xetra source files has started...')
        files = [
            key
            for date in self.extract_date_list
            for key in self.s3_bucket_src.list_files_in_prefix(prefix=date)
        ]
        if not files:
            df = pd.DataFrame()
        else:
            df = pd.concat([self.s3_bucket_src.read_csv_to_df(file_) for file_ in files], ignore_index=True)
        self._logger.info('Extracting Xetra source files has finished.')
        return df

    def transform_report1(self, df: pd.DataFrame):
        """
        Apply the necessary transformations to create report 1.
        :param df: pandas DataFrame with soure data

        :returns:
            df: a transformed pandas DataFrame
        """
        if df.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return df

        self._logger.info('Applying transformations to Xetra source data for report 1 started...')

        # Filter only the necessary columns
        df = df.loc[:, self.src_args.src_columns]
        df.dropna(inplace=True)

        # Calculate opening price per ISIN and day

        df[self.trg_args.col_opening_price] = (
            df
            .sort_values(by=self.src_args.src_col_time)
            .groupby([self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price]
            .transform('first')
        )

        # Calculate closing price per ISIN and day

        df[self.trg_args.col_closing_price] = (
            df
            .sort_values(by=self.src_args.src_col_time)
            .groupby([self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_end_price]
            .transform('last')
        )

        # Rename columns

        df.rename(
            columns={
                self.src_args.src_col_min_price: self.trg_args.col_min_price,
                self.src_args.src_col_max_price: self.trg_args.col_max_price,
                self.src_args.src_col_traded_vol: self.trg_args.col_daily_traded_volume
            },
            inplace=True
        )

        # Aggregate relevant columns

        df = (
            df
            .groupby([self.src_args.src_col_isin, self.src_args.src_col_date], as_index=False)
            .agg({
                self.trg_args.col_opening_price: 'min',
                self.trg_args.col_closing_price: 'min',
                self.trg_args.col_min_price: 'min',
                self.trg_args.col_max_price: 'max',
                self.trg_args.col_daily_traded_volume: 'sum'
            }
            )
        )

        # Change between current day's closing price to the previous trading day in %

        df[self.trg_args.col_change] = (
            df
            .sort_values(by=self.src_args.src_col_date)
            .groupby(self.src_args.src_col_isin)[self.trg_args.col_closing_price].shift(1)
        )
        df[self.trg_args.col_change] = \
            (df[self.trg_args.col_closing_price] - df[self.trg_args.col_change]) / df[self.trg_args.col_change] * 100

        # Round the change to 2 decimals
        df = df.round(decimals=2)

        # Remove the day before extract date
        df = df[df.Date >= self.extract_date].reset_index(drop=True)

        self._logger.info('Finished transformations of Xetra source data.')

        return df

    def load(self, df: pd.DataFrame):
        """
        Save a DataFrame to the target.

        :param df: a pandas DataFrame.
        """

        key = (
            f'{self.trg_args.key}'
            f'{datetime.today().strftime(self.trg_args.key_date_format)}.'
            f'{self.trg_args.format}'
        )

        # Write to target

        self.s3_bucket_trg.write_df_to_s3(df=df, key=key, file_format=self.trg_args.format)
        self._logger.info('Xetra target data is successfully written.')

        # Upload the meta file

        MetaProcess.update_meta_file(self.s3_bucket_trg, self.meta_key, self.meta_update_list)
        self._logger.info('Xetra meta file is successfully updated')
        return True

    def etl_report1(self):
        # Extract
        df = self.extract()

        # Transform
        df = self.transform_report1(df)

        # Load
        self.load(df)

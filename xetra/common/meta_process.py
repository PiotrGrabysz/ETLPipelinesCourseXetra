"""
Methods for processing the meta file.
"""

import collections
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd

from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.s3 import S3BucketConnector


class MetaProcess:
    """
    Class for working with the meta file.

    The class has two static method and no attributes. One could design them as two normal functions, but class oriented
    approach is taken here to be consistent with other parts of the project.
    """

    @staticmethod
    def update_meta_file(s3_bucket_meta: S3BucketConnector, meta_key: str, extract_date_list: List):
        # Create an empty DataFrame with column names predefined with the MetaProcessFormat enum.
        new_meta_file_df = pd.DataFrame(
            columns=[
                MetaProcessFormat.META_SOURCE_DATE_COL.value,
                MetaProcessFormat.META_PROCESS_COL.value
            ]
        )

        new_meta_file_df[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list

        # Fill the processed data column with current timestamp.
        # It adds this timestamp to every corresponding item from extract_date_list
        new_meta_file_df[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)

        try:
            # If some meta file already exists -> concatenate it with new meta data frame
            old_meta_file_df = s3_bucket_meta.read_csv_to_df(key=meta_key)

            # Check if the two meta files has the same columns (but an order of columns is not important)
            if collections.Counter(old_meta_file_df.columns) != collections.Counter(new_meta_file_df.columns):
                raise WrongMetaFileException(
                    f'Columns {old_meta_file_df.columns} and {new_meta_file_df.columns} does not match'
                )
            meta_file_all_df = pd.concat([old_meta_file_df, new_meta_file_df])

        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> use only the new data
            meta_file_all_df = new_meta_file_df

        # Write the meta file to S3
        s3_bucket_meta.write_df_to_s3(
            df=meta_file_all_df, key=meta_key, file_format=MetaProcessFormat.META_FILE_FORMAT.value
        )

    @staticmethod
    def return_date_list(
            s3_bucket_meta: S3BucketConnector,
            first_date: str,
            meta_key: str = 'meta_file.csv'
    ) -> Tuple[str, List[str]]:
        """
        Return dates that were not processed since `first_date` to today and the earliest such unprocessed date
        (min_date).

        NOTE: it returns dates ranging from `min_date` - 1 to today. It takes a day before `min_date` because later
        we want to calculate the percentage change in prices between each day, so in order to be able to compute
        change(%) in the day `min_date` we need to know the prices at the day before.

        If the meta_key does not exist in the bucket then it just returns a min_date=`first_date` and a list of
        consecutive days since `first_date` to today.

        If the meta_key exists in the bucket then `min_date` is minimal date of unprocessed dates later than `first_date`
        It returns `min_date` and a list of days since `min_date` to today that hasn't been processed yet. If all the
        dates has been processed then it returns `min_date` = future date (January 1, 2200) and an empty list.

        :param s3_bucket_meta: S3 bucket connector, for connecting with the bucket and reading the meta file.
        :param first_date: A string representing the date. It has to be in the format specified by
        xetra.common.constants.MetaProcessFormat.META_DATE_FORMAT.value
        :param meta_key: A key (name) of the meta file in the bucket.

        :returns
        minimal unprocessed date `min_date` and a list of unprocessed since `min_date` to today
        """

        # We have to take the day before in order to calculate percentage change of prices
        min_date = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
        today = datetime.today().date()

        try:
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            return_dates = [
                (min_date + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for x in range((today - min_date).days + 1)
            ]
            return_min_date = first_date
        else:
            dates = [min_date + timedelta(days=x) for x in range((today - min_date).days + 1)]
            src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)

            # Dates since `first_date` that were not in the meta file
            dates_missing = set(dates[1:]) - src_dates

            if dates_missing:
                min_date = min(dates_missing) - timedelta(days=1)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) for date in dates if date >= min_date]
                return_min_date = (min_date + timedelta(days=1)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            else:
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)

        return return_min_date, return_dates

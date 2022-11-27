"""
Test methods of MetaProcess class.
"""

from datetime import datetime, timedelta
import logging
from typing import Union

import pandas as pd
import pytest

from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector
from tests.common.s3_bucket_fixture import s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name, s3_bucket


def test_update_meta_file_no_previous_meta_file(s3_bucket):
    """
    Tests if the meta file is uploaded correctly when there hasn't been already any meta file in the bucket.
    """
    # Expected result
    date_list_exp = ['2022-11-24', '2022-11-25']
    proc_date_list_exp = [datetime.today().date()] * 2

    # Test init

    meta_file_key = 'new_metafile.csv'
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )
    extract_date_list = ['2022-11-24', '2022-11-25']

    # Method execution
    MetaProcess().update_meta_file(s3_bucket_conn, meta_file_key, extract_date_list)

    # Test after method execution

    meta_file_read = s3_bucket_conn.read_csv_to_df(meta_file_key)  # load the meta file back from the bucket
    date_list_result = meta_file_read[MetaProcessFormat.META_SOURCE_DATE_COL.value].tolist()
    proc_date_list_result = pd.to_datetime(
        meta_file_read[MetaProcessFormat.META_PROCESS_COL.value]
    ).dt.date.tolist()

    assert date_list_exp == date_list_result
    assert proc_date_list_exp == proc_date_list_result


def test_update_meta_file_ok(s3_bucket):
    """
    Test update_meta_file method when some older meta file has already existed in the bucket.
    """

    # Expected results

    date_list_old = ['2022-11-22', '2022-11-23']
    date_list_new = ['2022-11-24', '2022-11-25']
    date_list_exp = date_list_old + date_list_new
    proc_date_list_exp = [datetime.fromisoformat('2022-11-23').date()] * 2 + [datetime.today().date()] * 2

    # Test init - firstly upload an 'old' meta file

    meta_file_key = 'metafile.csv'
    meta_file_content = (
        f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n'  # header
        f'{date_list_old[0]},{proc_date_list_exp[0]}\n'
        f'{date_list_old[1]},{proc_date_list_exp[1]}'
    )
    s3_bucket.put_object(Body=meta_file_content, Key=meta_file_key)

    # Method execution - upload the new meta file

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    MetaProcess().update_meta_file(s3_bucket_conn, meta_file_key, date_list_new)

    # Load the meta file back from the bucket

    meta_file_read = s3_bucket_conn.read_csv_to_df(meta_file_key)  # load the meta file back from the bucket
    date_list_result = meta_file_read[MetaProcessFormat.META_SOURCE_DATE_COL.value].tolist()
    proc_date_list_result = pd.to_datetime(
        meta_file_read[MetaProcessFormat.META_PROCESS_COL.value]
    ).dt.date.tolist()

    # Test after method execution

    assert date_list_exp == date_list_result
    assert proc_date_list_exp == proc_date_list_result


def test_update_meta_file_empty_file(s3_bucket, caplog):
    """
    Test if it doesn't write a file if the extract_date_list is empty file
    and if it logs an appropriate message about empty file.
    """
    # Expected result
    log_exp = 'Attempted to write an empty data frame to the S3. No file will be written!'

    # Test init

    meta_file_key = 'metafile.csv'
    extract_date_list = []
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    with caplog.at_level(logging.INFO):
        MetaProcess().update_meta_file(s3_bucket_conn, meta_file_key, extract_date_list)
        log_msgs = [record.msg for record in caplog.records]
        assert log_exp in log_msgs


def test_update_meta_file_wrong_meta_file(s3_bucket):
    """
    Tests if update_meta_fie throws an error when trying to upload a meta file with
    different columns than a previous meta file
    """
    # Expected results

    date_list_old = ['2022-11-22', '2022-11-23']
    date_list_new = ['2022-11-24', '2022-11-25']
    proc_date_list_old = [datetime.fromisoformat('2022-11-23').date()] * 2
    proc_date_list_new = [datetime.today().date()] * 2

    # Test init - firstly upload an 'old' meta file

    meta_file_key = 'metafile.csv'
    # Create a meta file with different column names
    meta_file_content = (
        f'source_col2,proc_col2\n'  # header
        f'{date_list_old[0]},{proc_date_list_old[0]}\n'
        f'{date_list_old[1]},{proc_date_list_old[1]}'
    )
    s3_bucket.put_object(Body=meta_file_content, Key=meta_file_key)

    # Method execution - upload the new meta file

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    with pytest.raises(WrongMetaFileException):
        MetaProcess().update_meta_file(s3_bucket_conn, meta_file_key, date_list_new)


def dates_range(start: Union[str, datetime.date], end: datetime.date = datetime.today().date()):
    """
    An util function for testing return_date_list method. It returns a list of days since start to end. The returned
    days have type `str`, but have date like format.
    """
    if isinstance(start, str):
        start = datetime.strptime(start, MetaProcessFormat.META_DATE_FORMAT.value).date()
    date_list = [
        (start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        for x in range((end - start).days + 1)
    ]
    return date_list


def test_return_date_list_no_such_key(s3_bucket):
    """
    Tests return_date_list method in the case when there is no key `meta_key` in the bucket. In that case the method
    should return a list of consecutive days since a given `'first_date` until today.
    """
    # Expected results

    meta_key = 'meta_file.csv'
    first_date = '2022-11-22'
    min_date_exp = first_date

    min_date = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
    date_list_exp = dates_range(start=min_date)

    # Test init - firstly upload the meta file

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution

    return_min_date, return_dates = MetaProcess().return_date_list(s3_bucket_conn, first_date, meta_key)

    # Test after method execution

    assert return_min_date == min_date_exp
    assert return_dates == date_list_exp


def test_return_date_list_all_dates_are_done(s3_bucket):
    """
    Test return_date_list method in case when all the dates are already in the meta file. The method should return
    empty list.
    """

    # Expected results

    return_dates_exp = []
    return_min_date_exp = datetime(2200, 1, 1).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)

    # Test init - upload the meta file

    first_date = '2022-11-23'
    date_list = dates_range(start=first_date)
    proc_date_list = [datetime.today().date()] * len(date_list)

    meta_file_key = 'metafile.csv'
    meta_file_content = [
        f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n'  # header
    ]
    for date, proc_date in zip(date_list, proc_date_list):
        meta_file_content.append(f'{date},{proc_date}')
    meta_file_content = '\n'.join(meta_file_content)

    s3_bucket.put_object(Body=meta_file_content, Key=meta_file_key)

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution

    return_min_date, return_dates = MetaProcess().return_date_list(s3_bucket_conn, first_date, meta_file_key)

    assert return_min_date == return_min_date_exp
    assert return_dates == return_dates_exp


# Data for test_return_date_list_all_ok

first_date = (datetime.today() - timedelta(days=7)).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)
dates_list = dates_range(start=first_date)

first_date1 = first_date
min_date_exp1 = dates_list[-2]
dates_exp1 = dates_list[-3:]
dates_list1 = dates_list[:-2]

first_date2 = first_date
min_date_exp2 = dates_list[-1]
dates_exp2 = dates_list[-2:]
dates_list2 = dates_list[:-1]

first_date3 = (datetime.today() + timedelta(days=1)).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)
min_date_exp3 = datetime(2200, 1, 1).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)
dates_exp3 = []
dates_list3 = dates_list[:-1]

first_date4 = dates_list[4]
dates_list4 = dates_list[::2]
min_date_exp4 = dates_list[5]
dates_exp4 = dates_list[4:]

first_date5 = dates_list[5]
dates_list5 = dates_list[::2]
min_date_exp5 = dates_list[5]
dates_exp5 = dates_list[4:]


@pytest.mark.parametrize('first_date, dates_list, min_date_exp, dates_exp', [
    (first_date1, dates_list1, min_date_exp1, dates_exp1),
    (first_date2, dates_list2, min_date_exp2, dates_exp2),
    (first_date3, dates_list3, min_date_exp3, dates_exp3),
    (first_date4, dates_list4, min_date_exp4, dates_exp4),
    (first_date5, dates_list5, min_date_exp5, dates_exp5)
])
def test_return_date_list_all_ok(s3_bucket, first_date, dates_list, min_date_exp, dates_exp):
    """
    Test return_date_list method in case when some dates are not in the meta file.
    empty list.
    """

    # Expected results

    proc_date_list = [datetime.today().date()] * len(dates_list)

    # Test init - upload the meta file

    meta_file_key = 'metafile.csv'
    meta_file_content = [
        f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n'  # header
    ]
    for date, proc_date in zip(dates_list, proc_date_list):
        meta_file_content.append(f'{date},{proc_date}')
    meta_file_content = '\n'.join(meta_file_content)

    s3_bucket.put_object(Body=meta_file_content, Key=meta_file_key)

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution

    return_min_date, return_dates = MetaProcess().return_date_list(s3_bucket_conn, first_date, meta_file_key)

    print(f'{first_date=}')
    print(f'{dates_list=}')
    print(f'{min_date_exp=}')
    print(f'{dates_exp=}')

    assert return_min_date == min_date_exp
    assert return_dates == dates_exp


def test_return_date_list_wrong_col_names(s3_bucket):
    """Test return_date_list method when there is a wrong meta file."""

    # Test init - upload the meta file

    first_date = '2022-11-23'
    date_list = dates_range(start=first_date)
    proc_date_list = [datetime.today().date()] * len(date_list)

    meta_file_key = 'metafile.csv'
    meta_file_content = [
        f'wrong_col_name,{MetaProcessFormat.META_PROCESS_COL.value}\n'  # header
    ]
    for date, proc_date in zip(date_list, proc_date_list):
        meta_file_content.append(f'{date},{proc_date}')
    meta_file_content = '\n'.join(meta_file_content)

    s3_bucket.put_object(Body=meta_file_content, Key=meta_file_key)

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution

    with pytest.raises(KeyError):
        return_min_date, return_dates = MetaProcess().return_date_list(s3_bucket_conn, first_date, meta_file_key)

""" Test S3 bucket connector methods. """

from io import StringIO
import logging

import pandas as pd
import pytest

from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import WrongFormatException
from tests.common.s3_bucket_fixture import s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name, s3_bucket, my_s3_conn


def test_list_files_in_prefix_ok(s3_bucket, my_s3_conn):
    """
    Tests the list_files_in_prefix method for getting 2 file keys as list on the mocked s3 bucket.
    """

    # Expected results
    prefix_exp = 'prefix/'
    key1_exp = f'{prefix_exp}test1.csv'
    key2_exp = f'{prefix_exp}test2.csv'

    # Test init
    csv_content = """col1, col2
    valA,valB"""
    s3_bucket.put_object(Body=csv_content, Key=key1_exp)
    s3_bucket.put_object(Body=csv_content, Key=key2_exp)

    # Create a testing instance
    # s3_bucket_conn = S3BucketConnector(
    #     s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    # )

    # Method execution
    list_result = my_s3_conn.list_files_in_prefix(prefix_exp)

    # Tests after method execution
    assert len(list_result) == 2


def test_list_files_wrong_prefix(my_s3_conn):
    """
    Tests the list_files_in_prefix method in case of a
    wrong or not existing prefix.
    """
    # Expected results
    prefix_exp = 'no-prefix/'

    # Create a testing instance
    # s3_bucket_conn = S3BucketConnector(
    #     s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    # )

    # Method execution
    list_result = my_s3_conn.list_files_in_prefix(prefix_exp)

    # Tests after method execution
    assert not list_result


def test_read_csv_to_df(s3_bucket, caplog):
    """
    Tests the read_csv_to_df method if it correctly reads a csv file from the S3 bucket.
    """
    # Expected results
    key_exp = 'test.csv'
    csv_content = """col1,col2
            valA,valB
            valC,valD"""
    df_exp = pd.read_csv(StringIO(csv_content), sep=',')

    # Expected logs
    log_exp = f'Reading the {s3_endpoint_url}/{s3_bucket.name}/{key_exp}'

    # Test init
    s3_bucket.put_object(Body=csv_content, Key=key_exp)

    # Create a testing instance
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    with caplog.at_level(logging.INFO):
        df_result = s3_bucket_conn.read_csv_to_df(key_exp)
        for record in caplog.records:
            assert record.msg == log_exp

    # Tests after method execution
    pd.testing.assert_frame_equal(df_exp, df_result)


def test_write_df_to_s3_ok(s3_bucket, caplog):
    """
    Test if a dataframe uploaded to s3 and if it is the same after downloading it back.
    """

    # Expected results
    df_exp = pd.DataFrame(data={
        'col1': ['valA', 'valC'],
        'col2': ['valB', 'valD']
    })
    key_on_s3 = 'test.csv'
    log_exp = f'The data frame is written under the key={key_on_s3}'

    # Expected logs
    # log_exp = f'Reading the {s3_endpoint_url}/{s3_bucket.name}/{key_exp}'

    # Test init

    file_format = 'csv'

    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution

    with caplog.at_level(logging.INFO):
        s3_bucket_conn.write_df_to_s3(df_exp, key=key_on_s3, file_format=file_format)
        for record in caplog.records:
            assert record.msg == log_exp

    df_read = s3_bucket_conn.read_csv_to_df(key=key_on_s3)

    # Tests after method execution
    pd.testing.assert_frame_equal(df_exp, df_read)


@pytest.mark.skip('This feature is currently not implemented.')
def test_write_df_to_s3_existing_key(s3_bucket, caplog):
    """
    Test if the write_df_to_s3 method handles the case when the requested key already exists in the bucket.
    """
    # Expected results
    df_exp = pd.DataFrame(data={
        'col1': ['valA', 'valC'],
        'col2': ['valB', 'valD']
    })
    # log_exp = f'The data frame is written under the key={key_exp}'

    # Expected logs
    # log_exp = f'Reading the {s3_endpoint_url}/{s3_bucket.name}/{key_exp}'

    # Test init

    key_on_s3 = 'test.csv'
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    with caplog.at_level(logging.INFO):
        # Write the data twice under the same key
        s3_bucket_conn.write_df_to_s3(df_exp, key=key_on_s3, file_format='csv')
        s3_bucket_conn.write_df_to_s3(df_exp, key=key_on_s3, file_format='csv')

        for record in caplog.records:
            # assert record.msg == log_exp
            pass


def test_write_df_to_s3_empty_df(s3_bucket, caplog):
    """
    Tests write_df_to_s3 method if it doesn't write an empty data frame is passed.
    """
    # Expected value
    log_exp = 'Attempted to write an empty data frame to the S3. No file will be written!'
    result_exp = None

    # Test init

    df = pd.DataFrame()

    key_on_s3 = 'test.csv'
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    with caplog.at_level(logging.INFO):
        result = s3_bucket_conn.write_df_to_s3(df, key=key_on_s3, file_format='csv')
        for record in caplog.records:
            assert record.msg == log_exp

    # Assert that a file with the key 'key_on_s3' does not exists
    assert len(s3_bucket_conn.list_files_in_prefix(prefix=key_on_s3)) == 0

    assert result == result_exp


def test_write_df_to_s3_wrong_file_format(s3_bucket):
    """
    Test if the write_df_to_s3 method rejects file_formats other than .csv and .parquet
    and raises an error.

    This test is not passed when the data frame is empty, because the method write_df_to_s3
    returns when an empty data frame is being passed.
    """

    # Test init

    df = pd.DataFrame(data={
        'col1': ['valA', 'valC'],
        'col2': ['valB', 'valD']
    })

    key_on_s3 = 'test.csv'
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    with pytest.raises(WrongFormatException):
        s3_bucket_conn.write_df_to_s3(df, key=key_on_s3, file_format='jpg')

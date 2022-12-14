import os

import boto3
from moto import mock_s3
import pytest

from tests.transformers.xetra_data import df_src, df_report
from xetra.common.s3 import S3BucketConnector

s3_access_key = 'AWS_ACCESS_KEY_ID'
s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
s3_bucket_src_name = 'src-bucket'
s3_bucket_trg_name = 'trg-bucket'


@pytest.fixture
def s3_bucket_src():
    """Pytest fixture that creates the bucket in the fake moto AWS account.
    Yields a fake boto3 bucket.
    """
    with mock_s3():
        # Create s3 access key as environment variables
        os.environ[s3_access_key] = 'KEY1'
        os.environ[s3_secret_key] = 'KEY2'

        # Create a bucket on the mocked s3
        s3 = boto3.resource(service_name='s3', endpoint_url=s3_endpoint_url)
        s3.create_bucket(
            Bucket=s3_bucket_src_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'
            }
        )
        s3_bucket = s3.Bucket(s3_bucket_src_name)

        yield s3_bucket


@pytest.fixture
def s3_bucket_trg():
    """Pytest fixture that creates the bucket in the fake moto AWS account.
    Yields a fake boto3 bucket.
    """
    with mock_s3():
        # Create s3 access key as environment variables
        os.environ[s3_access_key] = 'KEY1'
        os.environ[s3_secret_key] = 'KEY2'

        # Create a bucket on the mocked s3
        s3 = boto3.resource(service_name='s3', endpoint_url=s3_endpoint_url)
        s3.create_bucket(
            Bucket=s3_bucket_trg_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'
            }
        )
        s3_bucket = s3.Bucket(s3_bucket_trg_name)

        yield s3_bucket


@pytest.fixture
def s3_bucket_src_connector(s3_bucket_src):
    s3_bucket_src_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_src_name
    )

    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[0:0],
    #     '2022-11-15/2022-11-15_BINS_XETR12.csv',
    #     'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[1:1],
    #     '2022-11-16/2022-11-16_BINS_XETR15.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[2:2],
    #     '2022-11-17/2022-11-17_BINS_XETR13.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[3:3],
    #     '2022-11-17/2022-11-17_BINS_XETR14.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[4:4],
    #     '2022-11-18/2022-11-18_BINS_XETR07.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[5:5],
    #     '2022-11-18/2022-11-18_BINS_XETR08.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[6:6],
    #     '2022-11-19/2022-11-19_BINS_XETR07.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[7:7],
    #     '2022-11-19/2022-11-19_BINS_XETR08.csv', 'csv'
    # )
    # s3_bucket_src_conn.write_df_to_s3(
    #     df_src.loc[8:8],
    #     '2022-11-19/2022-11-19_BINS_XETR09.csv', 'csv'
    # )
    yield s3_bucket_src_conn


@pytest.fixture
def s3_bucket_trg_connector(s3_bucket_trg):
    s3_bucket_trg_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_trg_name
    )
    yield s3_bucket_trg_conn

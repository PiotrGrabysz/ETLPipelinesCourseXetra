""" Test S3 bucket connector methods. """

import os

import boto3
from moto import mock_s3
import pytest

from xetra.common.s3 import S3BucketConnector

s3_access_key = 'AWS_ACCESS_KEY_ID'
s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
s3_bucket_name = 'test-bucket'


@pytest.fixture
def s3_bucket():
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
            Bucket=s3_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'
            }
        )
        s3_bucket = s3.Bucket(s3_bucket_name)

        yield s3_bucket


def test_list_files_in_prefix_ok(s3_bucket):
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
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    list_result = s3_bucket_conn.list_files_in_prefix(prefix_exp)

    # Tests after method execution
    assert len(list_result) == 2


def test_list_files_wrong_prefix(s3_bucket):
    """
    Tests the list_files_in_prefix method in case of a
    wrong or not existing prefix.
    """
    # Expected results
    prefix_exp = 'no-prefix/'

    # Create a testing instance
    s3_bucket_conn = S3BucketConnector(
        s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_name
    )

    # Method execution
    list_result = s3_bucket_conn.list_files_in_prefix(prefix_exp)

    # Tests after method execution
    assert not list_result

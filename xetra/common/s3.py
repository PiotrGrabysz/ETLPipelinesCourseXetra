"""
Connector and methods accessing AWS S3
"""

from io import BytesIO, StringIO
import logging
import os

import boto3
import pandas as pd


class S3BucketConnector:
    """
    Class for interacting with S3 Buckets
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket_name: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket_name: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(
            aws_access_key_id=os.environ[access_key],
            aws_secret_access_key=os.environ[secret_key]
        )
        self._s3 = self.session.resource(
            service_name='s3',
            endpoint_url=endpoint_url
        )
        self._bucket = self._s3.Bucket(bucket_name)

    def list_files_in_prefix(self, prefix: str):
        """
        List all the files in the S3 bucket starting with a prefix.

        :param prefix: prefix on S3 bucket that should be filtered with
        :return: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, decoding: str = 'utf-8', sep: str = ','):
        csv_obj = (
            self._bucket
            .Object(key=key)
            .get()
            .get('Body')
            .read()
            .decode(decoding)
        )
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, df: pd.DataFrame, key: str, mode: str):
        if mode == "parquet":
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
        elif mode == "csv":
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)

        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)


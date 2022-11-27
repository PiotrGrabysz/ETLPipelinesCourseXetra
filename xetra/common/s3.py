"""
Connector and methods accessing AWS S3
"""

from io import BytesIO, StringIO
import logging
import os

import boto3
import pandas as pd

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class S3BucketConnector:
    """
    Class for interacting with S3 Buckets
    """
    # TODO currently there is no way to ready parquet data (although you can save one)

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
            endpoint_url=self.endpoint_url
        )
        self._bucket = self._s3.Bucket(bucket_name)

        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket_name = bucket_name

    def __repr__(self):
        return (
            f"S3BucketConnector(access_key='{self._access_key}', secret_key='{self._secret_key}', "
            f"endpoint_url='{self.endpoint_url}', bucket_name='{self._bucket_name}')"
        )

    def list_files_in_prefix(self, prefix: str):
        """
        List all the files in the S3 bucket starting with a prefix.

        :param prefix: prefix on S3 bucket that should be filtered with
        :return: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        Fetch a .csv object from the bucket and convert it a pandas DataFrame.

        :param key: A key of the .csv object that should be read.
        :param encoding: Encoding of the data inside the csv file.
        :param sep: A separator used by pandas read_csv.

        returns:
            df: pandas DataFrame containing the data of the .csv file.
        """
        self._logger.info(f'Reading the {self.endpoint_url}/{self._bucket.name}/{key}')
        csv_obj = (
            self._bucket
            .Object(key=key)
            .get()
            .get('Body')
            .read()
            .decode(encoding)
        )
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        Write a data frame into a S3 bucket.

        :param df: A pandas Data Frame to be written.
        :param key: Key (name) of the saved file.
        :param file_format: format of the saved file. It has to be of the following: {'csv', 'parquet'}.

        :raises
        WrongFormatException, if the file_format is not supported
        """

        # TODO check if such a key already exists before writing it

        if df.empty:
            self._logger.info('Attempted to write an empty data frame to the S3. No file will be written!')
            return
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
        elif file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
        else:
            self._logger.info(
                f"The file format {file_format} is not supported. It should be either 'csv' or 'parquet'"
            )
            raise WrongFormatException

        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        self._logger.info(f'The data frame is written under the key={key}')

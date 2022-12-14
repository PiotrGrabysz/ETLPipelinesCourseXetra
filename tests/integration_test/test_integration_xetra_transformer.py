""" Test methods of xetra.transformers.xetra_transformer.XetraETL. """

import datetime
from io import BytesIO
import logging
from pathlib import Path

import boto3
import pandas as pd
import pytest
import yaml

# from tests.transformers.s3_bucket_fixture import buckets
# from tests.transformers.xetra_data import conf_dict_src, conf_dict_trg, df_src, df_report
from tests.integration_test.data import DataCreator
from xetra.transformers.xetra_transformer import XetraETL, XetraTargetConfig, XetraSourceConfig
from xetra.common.s3 import S3BucketConnector


stream_handler = logging.StreamHandler()
logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def source_bucket():
    # Before test - create resource
    config = yaml.safe_load(open(Path('tests/integration_test/etl_config.yml')))
    s3 = boto3.resource(service_name='s3', endpoint_url=config['s3']['src_endpoint_url'])
    src_bucket = s3.Bucket(config['s3']['src_bucket'])
    yield src_bucket

    # After test - remove resource
    for key in src_bucket.objects.all():
        key.delete()


@pytest.fixture
def target_bucket():
    # Before test - create resource
    config = yaml.safe_load(open(Path('tests/integration_test/etl_config.yml')))
    s3 = boto3.resource(service_name='s3', endpoint_url=config['s3']['trg_endpoint_url'])
    src_bucket = s3.Bucket(config['s3']['trg_bucket'])
    yield src_bucket

    # After test - remove resource
    for key in src_bucket.objects.all():
        key.delete()


def test_integration_etl_report1_no_metafile(source_bucket, target_bucket):
    """
    Integration test for the etl_report1 method in a case when there is no metafile
    and there should be created a new one.
    """

    config = yaml.safe_load(open(Path('tests/integration_test/etl_config.yml')))
    src_bucket = source_bucket
    trg_bucket = target_bucket

    s3_bucket_src = S3BucketConnector(
        config['s3']['access_key'],
        config['s3']['secret_key'],
        config['s3']['src_endpoint_url'],
        config['s3']['src_bucket']
    )
    s3_bucket_trg = S3BucketConnector(
        config['s3']['access_key'],
        config['s3']['secret_key'],
        config['s3']['trg_endpoint_url'],
        config['s3']['trg_bucket']
    )

    data = DataCreator()
    data.populate_date_to_s3(s3_bucket_src)

    # Expected results

    df_exp = data.df_report
    meta_exp = [data.dates[3], data.dates[2], data.dates[1], data.dates[0]]

    # Method execution

    meta_key = config['meta']['meta_key']
    source_config = config['source']
    source_config['first_extract_date'] = data.dates[3]
    source_config = XetraSourceConfig(**source_config)
    target_config = XetraTargetConfig(**config['target'])

    xetra_etl = XetraETL(
        s3_bucket_src,
        s3_bucket_trg,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    xetra_etl.etl_report1()

    # Test after method execution

    trg_file = s3_bucket_trg.list_files_in_prefix(target_config.key)[0]
    data = trg_bucket.Object(key=trg_file).get().get('Body').read()
    out_buffer = BytesIO(data)
    df_result = pd.read_parquet(out_buffer)
    pd.testing.assert_frame_equal(df_exp, df_result)

    meta_file = s3_bucket_trg.list_files_in_prefix(meta_key)[0]
    df_meta_result = s3_bucket_trg.read_csv_to_df(meta_file)
    assert df_meta_result['source_date'].tolist() == meta_exp

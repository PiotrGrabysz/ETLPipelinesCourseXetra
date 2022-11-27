""" Test methods of xetra.transformers.xetra_transformer.XetraETL. """

from datetime import datetime
from io import BytesIO
import logging

import pandas as pd

from tests.transformers.xetra_data import conf_dict_src, conf_dict_trg, df_src, df_report

from tests.transformers.s3_bucket_fixture import buckets
from tests.transformers.s3_bucket_fixture_old import s3_access_key, s3_secret_key, s3_endpoint_url, s3_bucket_src_name, s3_bucket_trg_name
from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraTargetConfig, XetraSourceConfig

meta_key = 'meta_file'


def test_extract_no_files(buckets):
    """ Test extract method when there are no files to be extracted. """

    # Test init
    extract_date = '2200-01-01'
    conf_dict_src['src_first_extract_date'] = extract_date

    s3_bucket_src_connector, s3_bucket_trg_connector = buckets

    # Method execution

    source_config = XetraSourceConfig(**conf_dict_src)
    target_config = XetraTargetConfig(**conf_dict_trg)

    xetra_etl1 = XetraETL(
        s3_bucket_src=s3_bucket_src_connector,
        s3_bucket_trg=s3_bucket_trg_connector,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    df_return = xetra_etl1.extract()

    assert df_return.empty


def test_extract_ok(buckets):
    # Expected results
    df_exp = df_src.loc[1:8].reset_index(drop=True)

    # Test init
    extract_date = '2022-11-17'

    conf_dict_src['src_first_extract_date'] = extract_date

    s3_bucket_src_connector, s3_bucket_trg_connector = buckets

    # Method execution

    source_config = XetraSourceConfig(**conf_dict_src)
    target_config = XetraTargetConfig(**conf_dict_trg)

    xetra_etl1 = XetraETL(
        s3_bucket_src=s3_bucket_src_connector,
        s3_bucket_trg=s3_bucket_trg_connector,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    df_return = xetra_etl1.extract()

    pd.testing.assert_frame_equal(df_return, df_exp)


def test_transform_report1_empty(buckets, caplog):
    """ Test transform_report1 in case when the input data frame is empty. """

    # Expected output

    log_exp = 'The dataframe is empty. No transformations will be applied.'

    # Test init

    extract_date = '2022-11-17'
    conf_dict_src['src_first_extract_date'] = extract_date
    df_input = pd.DataFrame()

    s3_bucket_src_connector, s3_bucket_trg_connector = buckets

    source_config = XetraSourceConfig(**conf_dict_src)
    target_config = XetraTargetConfig(**conf_dict_trg)

    # Method execution

    xetra_etl1 = XetraETL(
        s3_bucket_src=s3_bucket_src_connector,
        s3_bucket_trg=s3_bucket_trg_connector,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    with caplog.at_level(logging.INFO):
        df_return = xetra_etl1.transform_report1(df_input)
        for record in caplog.records:
            assert record.msg == log_exp

    pd.testing.assert_frame_equal(df_return, df_input)


def test_transform_report1(buckets, caplog):
    """ Test transform_report1. """

    # Expected output

    log_exp = [
        'Applying transformations to Xetra source data for report 1 started...',
        'Finished transformations of Xetra source data.'
    ]
    df_exp = df_report

    # Test init

    extract_date = '2022-11-17'
    conf_dict_src['src_first_extract_date'] = extract_date
    df_input = df_src.loc[1:8].reset_index(drop=True)

    s3_bucket_src_connector, s3_bucket_trg_connector = buckets

    source_config = XetraSourceConfig(**conf_dict_src)
    target_config = XetraTargetConfig(**conf_dict_trg)

    # Method execution

    xetra_etl1 = XetraETL(
        s3_bucket_src=s3_bucket_src_connector,
        s3_bucket_trg=s3_bucket_trg_connector,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    with caplog.at_level(logging.INFO):
        df_return = xetra_etl1.transform_report1(df_input)
        assert [record.msg for record in caplog.records] == log_exp

    pd.testing.assert_frame_equal(df_return, df_exp)


def test_load(buckets, caplog, monkeypatch):
    """ Test load method """
    # def mock_today():
    #     return datetime(year=2022, month=11, day=20).date
    #
    # monkeypatch.setattr(datetime, 'today', mock_today)

    # Expected output

    log_exp = [
        'Xetra target data is successfully written.',
        'Xetra meta file is successfully updated'
    ]
    df_exp = df_report
    meta_exp = [
        '2022-11-17', '2022-11-18', '2022-11-19', '2022-11-20', '2022-11-21', '2022-11-22', '2022-11-23', '2022-11-24',
        '2022-11-25', '2022-11-26', '2022-11-27'
    ]

    # Test init

    extract_date = '2022-11-17'
    conf_dict_src['src_first_extract_date'] = extract_date
    df_input = df_report

    s3_bucket_src_connector, s3_bucket_trg_connector = buckets

    source_config = XetraSourceConfig(**conf_dict_src)
    target_config = XetraTargetConfig(**conf_dict_trg)

    # Method execution

    xetra_etl1 = XetraETL(
        s3_bucket_src=s3_bucket_src_connector,
        s3_bucket_trg=s3_bucket_trg_connector,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    with caplog.at_level(logging.INFO):
        xetra_etl1.load(df_input)
        logs = [record.msg for record in caplog.records]
        assert log_exp[0] in logs
        assert log_exp[1] in logs

    # Test after method execution

    trg_file = s3_bucket_trg_connector.list_files_in_prefix(target_config.key)[0]
    data = s3_bucket_trg_connector._bucket.Object(key=trg_file).get().get('Body').read()
    out_buffer = BytesIO(data)
    df_result = pd.read_parquet(out_buffer)
    pd.testing.assert_frame_equal(df_result, df_exp)

    meta_file = s3_bucket_trg_connector.list_files_in_prefix(meta_key)[0]
    df_meta_result = s3_bucket_trg_connector.read_csv_to_df(meta_file)
    
    print(f"{df_meta_result['source_date'].tolist()=}")
    assert df_meta_result['source_date'].tolist() == meta_exp


def test_etl_report1(buckets):

    # Expected output

    df_exp = df_report
    meta_exp = [
        '2022-11-17', '2022-11-18', '2022-11-19', '2022-11-20', '2022-11-21', '2022-11-22', '2022-11-23', '2022-11-24',
        '2022-11-25', '2022-11-26', '2022-11-27'
    ]

    # Test init

    extract_date = '2022-11-17'
    conf_dict_src['src_first_extract_date'] = extract_date

    s3_bucket_src_connector, s3_bucket_trg_connector = buckets

    source_config = XetraSourceConfig(**conf_dict_src)
    target_config = XetraTargetConfig(**conf_dict_trg)

    # Method execution

    xetra_etl1 = XetraETL(
        s3_bucket_src=s3_bucket_src_connector,
        s3_bucket_trg=s3_bucket_trg_connector,
        meta_key=meta_key,
        src_args=source_config,
        trg_args=target_config
    )

    xetra_etl1.etl_report1()

    # Test after method execution

    trg_file = s3_bucket_trg_connector.list_files_in_prefix(target_config.key)[0]
    data = s3_bucket_trg_connector._bucket.Object(key=trg_file).get().get('Body').read()
    out_buffer = BytesIO(data)
    df_result = pd.read_parquet(out_buffer)
    pd.testing.assert_frame_equal(df_result, df_exp)

    meta_file = s3_bucket_trg_connector.list_files_in_prefix(meta_key)[0]
    df_meta_result = s3_bucket_trg_connector.read_csv_to_df(meta_file)

    print(f"{df_meta_result['source_date'].tolist()=}")
    assert df_meta_result['source_date'].tolist() == meta_exp

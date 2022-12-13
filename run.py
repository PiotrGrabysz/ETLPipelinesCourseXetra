"""
Running the Xetra ETL application
"""

import argparse
import logging
import logging.config
from pathlib import Path

import yaml

from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """
    Entry point ti run the xetra ETL job
    """
    # Parsing YAML file

    parser = argparse.ArgumentParser(description='Run the Xetra ETL job.')
    parser.add_argument('config', help='An YAML configuration file.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))

    # Configure logging

    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # Read S3 configuration and create connectors

    s3_config = config['s3']

    s3_bucket_src = S3BucketConnector(
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_key'],
        endpoint_url=s3_config['src_endpoint_url'],
        bucket_name=s3_config['src_bucket'],
    )

    s3_bucket_trg = S3BucketConnector(
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_key'],
        endpoint_url=s3_config['trg_endpoint_url'],
        bucket_name=s3_config['trg_bucket'],
    )

    # Read source configuration
    source_config = XetraSourceConfig(**config['source'])

    # Read target configuration
    target_config = XetraTargetConfig(**config['target'])

    # Read meta file configuration
    meta_config = config['meta']

    # Create ETL class instance
    logger.info('Xetra ETL job has started')
    xetra_etl = XetraETL(
        s3_bucket_src=s3_bucket_src,
        s3_bucket_trg=s3_bucket_trg,
        meta_key=meta_config['meta_key'],
        src_args=source_config,
        trg_args=target_config
    )

    # Run etl report1
    xetra_etl.etl_report1()
    logger.info('Xetra ETL job has finished. ')


if __name__ == '__main__':
    main()

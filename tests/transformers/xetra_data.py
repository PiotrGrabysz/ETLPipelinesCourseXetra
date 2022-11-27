""" Create some data to be used for testing the XetraETL pipeline. """

import pandas as pd

from xetra.transformers.xetra_transformer import XetraTargetConfig, XetraSourceConfig


# Creating source and target configuration
conf_dict_src = {
    'src_first_extract_date': '2022-11-17',
    'src_columns': [
        'ISIN', 'Mnemonic', 'Date', 'Time',
        'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'
    ],
    'src_col_date': 'Date',
    'src_col_isin': 'ISIN',
    'src_col_time': 'Time',
    'src_col_start_price': 'StartPrice',
    'src_col_end_price': 'EndPrice',
    'src_col_min_price': 'MinPrice',
    'src_col_max_price': 'MaxPrice',
    'src_col_traded_vol': 'TradedVolume'
}
conf_dict_trg = {
    'col_isin': 'isin',
    'col_date': 'date',
    'col_opening_price': 'opening_price_eur',
    'col_closing_price': 'closing_price_eur',
    'col_min_price': 'minimum_price_eur',
    'col_max_price': 'maximum_price_eur',
    'col_daily_traded_volume': 'daily_traded_volume',
    'col_change': 'change_prev_closing_%',
    'key': 'report1/xetra_daily_report1_',
    'key_date_format': '%Y%m%d_%H%M%S',
    'format': 'parquet'
}

# Creating source files on mocked s3
columns_src = [
    'ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'
]
data = [
    ['AT0000A0E9W5', 'SANT', '2022-11-15', '12:00', 20.19, 18.45, 18.20, 20.33, 877],
    ['AT0000A0E9W5', 'SANT', '2022-11-16', '15:00', 18.27, 21.19, 18.27, 21.34, 987],
    ['AT0000A0E9W5', 'SANT', '2022-11-17', '13:00', 20.21, 18.27, 18.21, 20.42, 633],
    ['AT0000A0E9W5', 'SANT', '2022-11-17', '14:00', 18.27, 21.19, 18.27, 21.34, 455],
    ['AT0000A0E9W5', 'SANT', '2022-11-18', '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
    ['AT0000A0E9W5', 'SANT', '2022-11-18', '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
    ['AT0000A0E9W5', 'SANT', '2022-11-19', '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
    ['AT0000A0E9W5', 'SANT', '2022-11-19', '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
    ['AT0000A0E9W5', 'SANT', '2022-11-19', '09:00', 24.22, 22.21, 22.21, 25.01, 1523]
]
df_src = pd.DataFrame(data, columns=columns_src)

columns_report = [
    'ISIN', 'Date', 'opening_price_eur', 'closing_price_eur', 'minimum_price_eur',
    'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%'
]
data_report = [
    ['AT0000A0E9W5', '2022-11-17', 20.21, 21.19, 18.21, 21.34, 1088, 0.0],
    ['AT0000A0E9W5', '2022-11-18', 20.58, 21.14, 18.89, 21.14, 10286, -0.24],
    ['AT0000A0E9W5', '2022-11-19', 23.58, 22.21, 22.21, 25.01, 3586, 5.06]
]
df_report = pd.DataFrame(data_report, columns=columns_report)

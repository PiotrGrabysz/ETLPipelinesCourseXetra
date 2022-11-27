# ETLPipelinesCourseXetra
A repository containing a code created during the Udemy course: *"Writing production ready ETL pipelines with Python".*

## Running the code

Run this command to start the environment:

```commandline
pipenv shell
```

## About the task and the data

The data used in this project is *Deutsche Börse Public Dataset*:

![](images/deutsche_borse_public_dataset_preview1.png)

A column name `ISIN` stands for *International Securities Identification Number* 
(wikipedia article: [https://en.wikipedia.org/wiki/International_Securities_Identification_Number](wiki)) 
The rest of names is self explanatory.

The goal is to aggregate data for each unique `ISIN` and get the values of:

* `opening_price`: it is the first StartPrice price of each unique ISIN per each day
* `closing_price`: it is the last EndPrice price of each unique ISIN per each day
* `min_price`: minimal price for each `ISIN` per day
* `max_price`: maximal price for each `ISIN` per day
* `daily_traded_valume`: sum of all volume per given day
* `change_prev_closing_%`: change in closing price between current day and the day before in %

## About testing

To check what part of the code has been covered with tests run:
```commandline
coverage run --omit *tests/* -m pytest

coverage report -m
```

### Integration tests specification

Currently, there is only one test provided:

| test name                       | test description                                     | test init                                                                    | input | expected ouput                                                      |
|---------------------------------|------------------------------------------------------|------------------------------------------------------------------------------|-------|---------------------------------------------------------------------|
| test_int_etl_report_no_metafile | Integration test for the main entrypoint etl_report1 | s3 source bucket with source content. Create S3BucketConnector test instance | -     | .parquet file on S3 with the source data content in report 1 format |

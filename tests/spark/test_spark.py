import pytest
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types

from spark_pipeline.monthly_transformation_script import monthly_data_transformation
from spark_pipeline.ingestion_script import get_schema_monthly


WORKING_DIR = Path('').resolve()
PATH_TEST_DIR = Path(WORKING_DIR, 'tests/test_data')


def test_spark_monthly_data_transformation(spark) -> None:
    """ Test the whole polar monthly data transformation
    """
    sample_df = (spark.read.option('header', True)
                 .schema(get_schema_monthly())
                 .csv(Path(PATH_TEST_DIR, 'zip9_coded_201904_pv_test.csv').as_posix())
                 .filter(F.col('zip9_code').isin([703023, 627746, 3779203, 802280, 3471270, 1478042]))
                 )

    demo_data_test_dir = Path(PATH_TEST_DIR, 'zip9_demographics_coded_pv_test.csv').as_posix()

    expected_df = (spark.read.option('header', True)
                   .schema(get_schema_monthly_processed())
                   .csv(Path(PATH_TEST_DIR, '201904_monthly_processed_data_expected.csv').as_posix())
                   )

    assert_spark_dataframe_equal(monthly_data_transformation(sample_df, spark, demo_data_dir=demo_data_test_dir),
                                 expected_df)
    return


def assert_spark_dataframe_equal(df_1, df_2):
    assert df_1.orderBy(F.asc(F.col('zip9_code'))).collect() == df_2.orderBy(F.asc(F.col('zip9_code'))).collect()

    # ignore nullable
    assert ([(field.name, field.dataType) for field in df_1.schema.fields] ==
            [(field.name, field.dataType) for field in df_2.schema.fields])
    return


def get_schema_monthly_processed():
    schema = types.StructType([
        types.StructField('zip5', types.StringType(), False),
        types.StructField('zip9_code', types.IntegerType(), False),
        types.StructField('bankcard_limit', types.DoubleType(), True),
        types.StructField('bankcard_balance', types.DoubleType(), True),
        types.StructField('bankcard_trades', types.DoubleType(), True),
        types.StructField('bankcard_util', types.DoubleType(), True),
        types.StructField('total_revolving_limit', types.DoubleType(), True),
        types.StructField('total_revolving_balance', types.DoubleType(), True),
        types.StructField('total_revolving_trades', types.DoubleType(), True),
        types.StructField('total_revolving_util', types.DoubleType(), True),
        types.StructField('mortgage1_limit', types.DoubleType(), True),
        types.StructField('mortgage1_balance', types.DoubleType(), True),
        types.StructField('mortgage1_open', types.DoubleType(), True),
        types.StructField('mortgage2_limit', types.DoubleType(), True),
        types.StructField('mortgage2_balance', types.DoubleType(), True),
        types.StructField('mortgage2_open', types.DoubleType(), True),
        types.StructField('mortgage3_limit', types.DoubleType(), True),
        types.StructField('mortgage3_balance', types.DoubleType(), True),
        types.StructField('mortgage3_open', types.DoubleType(), True),
        types.StructField('mortgage4_limit', types.DoubleType(), True),
        types.StructField('mortgage4_balance', types.DoubleType(), True),
        types.StructField('mortgage4_open', types.DoubleType(), True),
        types.StructField('mortgage5_limit', types.DoubleType(), True),
        types.StructField('mortgage5_balance', types.DoubleType(), True),
        types.StructField('mortgage5_open', types.DoubleType(), True),
        types.StructField('total_mortgage_limit', types.DoubleType(), True),
        types.StructField('total_mortgage_balance', types.DoubleType(), True),
        types.StructField('total_mortgage_trades', types.DoubleType(), True),
        types.StructField('mortgage1_loan_to_value', types.DoubleType(), True),
        types.StructField('homeequity1_limit', types.DoubleType(), True),
        types.StructField('homeequity1_balance', types.DoubleType(), True),
        types.StructField('homeequity1_open', types.DoubleType(), True),
        types.StructField('homeequity2_limit', types.DoubleType(), True),
        types.StructField('homeequity2_balance', types.DoubleType(), True),
        types.StructField('homeequity2_open', types.DoubleType(), True),
        types.StructField('homeequity3_limit', types.DoubleType(), True),
        types.StructField('homeequity3_balance', types.DoubleType(), True),
        types.StructField('homeequity3_open', types.DoubleType(), True),
        types.StructField('homeequity4_limit', types.DoubleType(), True),
        types.StructField('homeequity4_balance', types.DoubleType(), True),
        types.StructField('homeequity4_open', types.DoubleType(), True),
        types.StructField('homeequity5_limit', types.DoubleType(), True),
        types.StructField('homeequity5_balance', types.DoubleType(), True),
        types.StructField('homeequity5_open', types.DoubleType(), True),
        types.StructField('total_homeequity_limit', types.DoubleType(), True),
        types.StructField('total_homeequity_balance', types.DoubleType(), True),
        types.StructField('total_homeequity_trades', types.DoubleType(), True),
        types.StructField('homeequity1_loan_to_value', types.DoubleType(), True),
        types.StructField('autoloan_open', types.DoubleType(), True),
        types.StructField('studentloan_open', types.DoubleType(), True),
        types.StructField('bankcard_open', types.DoubleType(), True),
        types.StructField('homeequity_open', types.DoubleType(), True),
        types.StructField('mortgage_open', types.DoubleType(), True),
        types.StructField('age', types.DoubleType(), True),
        types.StructField('household_count', types.IntegerType(), True),
        types.StructField('person_count', types.IntegerType(), True),
        types.StructField('homebuyers', types.IntegerType(), True),
        types.StructField('first_homebuyers', types.IntegerType(), True),
        # new fields
        types.StructField('count_mtg_event', types.IntegerType(), True),
        types.StructField('count_heq_event', types.IntegerType(), True),
        types.StructField('mtg_heq_valid_flag', types.BooleanType(), True),
        types.StructField('state', types.StringType(), True),
        types.StructField('major_city', types.StringType(), True),
    ])
    return schema

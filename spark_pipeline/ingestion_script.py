
import os
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from general_utils import time_function
from spark_pipeline.monthly_transformation_script import monthly_data_transformation


@time_function
def monthly_data_ingestion(input_path: str, output_path: str, ingestion_mode: str, input_year: int, month: int,
                           test_run: str, spark: SparkSession) -> None:
    """ ingestion process for the monthly data
    """
    # read
    if test_run:
        df = spark.read.option('header', True) \
            .schema(get_schema_monthly()) \
            .csv(input_path).limit(2000)
    else:
        df = spark.read.option('header', True) \
            .schema(get_schema_monthly()) \
            .csv(input_path)

    df = monthly_data_transformation(df, spark)

    # write
    df.repartition(6).write.format("parquet") \
        .mode(ingestion_mode) \
        .save(output_path)

    # report
    generate_monthly_report(df, output_path, input_year, month, spark)
    return


@time_function
def generate_monthly_report(df: DataFrame, output_path: str, input_year: int, month: int, spark: SparkSession) -> None:
    """ generate a single aggregated monthly report (csv) in the directory.
    """
    df.createOrReplaceTempView('monthly_credit_score_tmp')
    df_result = spark.sql("""
    SELECT zip5 AS zip5, state AS state
        , ROUND(SUM(bankcard_limit * person_count) / SUM(person_count), 3) AS bankcard_limit_avg
        , ROUND(SUM(bankcard_balance * person_count) / SUM(person_count), 3) AS bankcard_balance_avg
        , ROUND(SUM(bankcard_trades * person_count) / SUM(person_count), 3) AS bankcard_trades_avg
        , COUNT(zip9_code) AS zip9_code_count
        , array_join(collect_set(major_city), ', ') AS covered_major_cities
        , SUM(household_count) AS household_count_total
        , SUM(person_count) AS person_count_total
        , SUM(homebuyers) AS homebuyers_total
        , SUM(first_homebuyers) AS first_homebuyers_total
    FROM monthly_credit_score_tmp
    GROUP BY zip5, state
    """)

    # create report subdirectory for the report CSV
    output_path_report = output_path + '/report/'
    if not os.path.exists(output_path_report):
        os.mkdir(output_path_report)

    df_result.toPandas().to_csv(output_path_report + f'{input_year}{month:02d}_monthly_report.csv',
                                index=False)

    # show examples in log
    df_result.show(5)
    return


def get_schema_monthly() -> types.StructType:
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
    ])
    return schema


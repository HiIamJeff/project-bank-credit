
import os
import polars as pl

from general_utils import time_function
from polars_pipeline.monthly_transformation_script import monthly_data_transformation


@time_function
def monthly_data_ingestion(input_path, output_path, input_year, month, test_run=False):
    """ ingestion process for the monthly data
    """
    # read
    if test_run:
        df = pl.scan_csv(input_path, dtypes=get_schema_monthly()).head(2000)

    else:
        df = pl.scan_csv(input_path, dtypes=get_schema_monthly())

    df = monthly_data_transformation(df)

    # write
    # create subdirectory
    output_path_report = output_path + '/report/'
    if not os.path.exists(output_path_report):
        os.mkdir(output_path_report)

    # query plan
    # with open('query_plan2.txt', 'w') as f:
    #     f.write(df.explain())

    df.collect().write_parquet(output_path + f'/{input_year}{month:02d}_monthly_processed_data.parquet')

    # report
    generate_monthly_report(df, output_path, input_year, month)
    return


def get_schema_monthly():
    dict_schema = {
        'zip5': pl.Utf8,
        'zip9_code': pl.Int32,
        'bankcard_limit': pl.Float32,
        'bankcard_balance': pl.Float32,
        'bankcard_trades': pl.Float32,
        'bankcard_util': pl.Float32,
        'total_revolving_limit': pl.Float32,
        'total_revolving_balance': pl.Float32,
        'total_revolving_trades': pl.Float32,
        'total_revolving_util': pl.Float32,
        'mortgage1_limit': pl.Float32,
        'mortgage1_balance': pl.Float32,
        'mortgage1_open': pl.Float32,
        'mortgage2_limit': pl.Float32,
        'mortgage2_balance': pl.Float32,
        'mortgage2_open': pl.Float32,
        'mortgage3_limit': pl.Float32,
        'mortgage3_balance': pl.Float32,
        'mortgage3_open': pl.Float32,
        'mortgage4_limit': pl.Float32,
        'mortgage4_balance': pl.Float32,
        'mortgage4_open': pl.Float32,
        'mortgage5_limit': pl.Float32,
        'mortgage5_balance': pl.Float32,
        'mortgage5_open': pl.Float32,
        'total_mortgage_limit': pl.Float32,
        'total_mortgage_balance': pl.Float32,
        'total_mortgage_trades': pl.Float32,
        'mortgage1_loan_to_value': pl.Float32,
        'homeequity1_limit': pl.Float32,
        'homeequity1_balance': pl.Float32,
        'homeequity1_open': pl.Float32,
        'homeequity2_limit': pl.Float32,
        'homeequity2_balance': pl.Float32,
        'homeequity2_open': pl.Float32,
        'homeequity3_limit': pl.Float32,
        'homeequity3_balance': pl.Float32,
        'homeequity3_open': pl.Float32,
        'homeequity4_limit': pl.Float32,
        'homeequity4_balance': pl.Float32,
        'homeequity4_open': pl.Float32,
        'homeequity5_limit': pl.Float32,
        'homeequity5_balance': pl.Float32,
        'homeequity5_open': pl.Float32,
        'total_homeequity_limit': pl.Float32,
        'total_homeequity_balance': pl.Float32,
        'total_homeequity_trades': pl.Float32,
        'homeequity1_loan_to_value': pl.Float32,
        'autoloan_open': pl.Float32,
        'studentloan_open': pl.Float32,
        'bankcard_open': pl.Float32,
        'homeequity_open': pl.Float32,
        'mortgage_open': pl.Float32,
    }
    return dict_schema


@time_function
def generate_monthly_report(df, output_path, input_year, month):
    """ generate a single aggregated monthly report (csv) in the directory.
    """
    df_result = (
        df.groupby(['zip5', 'state'])
        .agg([
            ((pl.col("bankcard_limit") * pl.col('person_count')).sum() / pl.col('person_count').sum())
            .round(3).alias('bankcard_limit_avg'),
            ((pl.col("bankcard_balance") * pl.col('person_count')).sum() / pl.col('person_count').sum())
            .round(3).alias('bankcard_balance_avg'),
            ((pl.col("bankcard_trades") * pl.col('person_count')).sum() / pl.col('person_count').sum())
            .round(3).alias('bankcard_trades'),
            pl.col('zip9_code').count().alias('zip9_code_count'),
            pl.col('household_count').sum().alias('household_count_total'),
            pl.col('person_count').sum().alias('person_count_total'),
            pl.col('homebuyers').sum().alias('homebuyers_total'),
            pl.col('first_homebuyers').sum().alias('first_homebuyers_total'),
            pl.col("major_city").unique().alias("covered_major_cities")
        ])
        .with_columns([
            pl.col("covered_major_cities").list.join(", ") # turn list into string
        ])
        .sort(pl.col('zip5'), descending=False)
    )

    # create report subdirectory for the report CSV
    output_path_report = output_path + '/report/'
    if not os.path.exists(output_path_report):
        os.mkdir(output_path_report)

    df_result_materialized = df_result.collect()
    df_result_materialized.write_csv(output_path_report + f'{input_year}{month:02d}_monthly_report.csv')

    # show examples in log
    pl.Config.set_tbl_rows(20)
    # print(df_result.fetch(5)[0, ].melt())
    print(df_result_materialized[0, ].melt())
    return

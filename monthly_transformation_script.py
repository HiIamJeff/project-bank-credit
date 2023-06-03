
import warnings
# suppress warnings from uszipcode
warnings.filterwarnings("ignore", message="Using slow pure-python SequenceMatcher")

from uszipcode import SearchEngine
from pyspark.sql import functions as F
from pyspark.sql import types


def monthly_data_transformation(df, spark):
    """
    Process monthly data
    return new data for analysis and future steps (ML pipeline)
    """
    # join demographic table
    demo_data_dir = 'data/source/demographic_data/zip9_demographics_coded_pv.csv'
    df_demo = (spark.read
               .option('header', True)
               .schema(get_schema_demo())
               .csv(demo_data_dir))

    df = df.alias('df')
    df_demo = df_demo.alias('df_demo')
    df_merged = df.join(df_demo, df.zip9_code == df_demo.zip9_code, 'inner')
    df_merged = (df_merged.select([F.col('df.*')] +
                                  [F.col('df_demo.' + c) for c in df_demo.columns if c not in ['zip5', 'zip9_code']]))

    # imputation
    df_merged = impute_monthly_data(df_merged)

    # generate new columns
    udf_map_geo = get_udf_map_geo()
    df_merged = (df_merged
                 .withColumn("geo_state_major_city", F.explode(F.array(udf_map_geo(F.col("zip5")))))
                 .withColumn('count_mtg_event', count_event_mtg_expr())
                 .withColumn('count_heq_event', count_event_heq_expr())
                 .withColumn("mtg_heq_valid_flag", F.when(generate_mtg_heq_valid_bool_expr(), True).otherwise(False)))

    df_merged = (df_merged.select(F.col('*'))
                 .withColumn('state', F.col('geo_state_major_city.state'))
                 .withColumn('major_city', F.col('geo_state_major_city.major_city'))
                 .drop(F.col('geo_state_major_city')))
    return df_merged


def get_schema_demo():
    schema_demo = types.StructType([
        types.StructField('zip5', types.StringType(), False),
        types.StructField('zip9_code', types.IntegerType(), False),
        types.StructField('age', types.DoubleType(), True),
        types.StructField('household_count', types.IntegerType(), True),
        types.StructField('person_count', types.IntegerType(), True),
        types.StructField('homebuyers', types.IntegerType(), True),
        types.StructField('first_homebuyers', types.IntegerType(), True),
    ])
    return schema_demo


def impute_monthly_data(df):
    """ Basic imputation for monthly data (excluding specific columns)
    """
    list_num_col = [f.name for f in df.schema.fields if isinstance(f.dataType, (types.DoubleType, types.IntegerType))]
    list_num_exception = ['bankcard_util', 'total_revolving_util', 'mortgage1_loan_to_value']
    list_num_col = [c for c in list_num_col if c not in list_num_exception]

    dict_na = {c: 0 for c in list_num_col}
    df = df.fillna(dict_na)
    return df


def get_udf_map_geo():
    """ Generate udf for basic geo information
    """
    dict_zipcode = get_zipcode_dictionary()
    schema_udf = types.StructType([
        types.StructField("state", types.StringType(), True),
        types.StructField("major_city", types.StringType(), True)
    ])
    return F.udf(lambda zipcode: dict_zipcode[zipcode], schema_udf)


def get_zipcode_dictionary():
    """ Use uszipcode package to generate mapping (state and major city associated with all zip codes)
    """
    zipcodes = SearchEngine().query(zipcode_type=None, returns=100000)
    dict_zipcode = {z.zipcode: (z.state, z.major_city) for z in zipcodes}
    return dict_zipcode


def generate_mtg_heq_valid_bool_expr():
    """ Generate query expression for identifying problematic records based on mortgage/home equity columns
    The logic is that the columns should either have all meaningful numbers (about limit, balance, open) or not at all to make a valid info
    (e.g., a record with any missing info for one specific loan will be labeled as an invalid record, which is False)
    p.s. The logic applies to all 5 sets of columns with mortgages and home equities
    """
    list_expr_all = []
    for list_col_set in [['mortgage1_limit', 'mortgage1_balance', 'mortgage1_open'],
                         ['homeequity1_limit', 'homeequity1_balance', 'homeequity1_open']]:
        list_col_full = []
        for i in range(1, 6):
            list_col_full.append([col.replace(str(1), str(i)) for col in list_col_set])

        list_expr = []
        for col_list in list_col_full:
            # either all columns have valid numbers or not
            # (potential NULL values should be imputed as 0 in previous steps)
            expr = ((F.col(col_list[0]) != 0)
                    & (F.col(col_list[1]) != 0)
                    & (F.col(col_list[2]) != 0)) | ((F.col(col_list[0]) == 0)
                                                    & (F.col(col_list[1]) == 0)
                                                    & (F.col(col_list[2]) == 0))
            list_expr.append(expr)
        list_expr_all.append(list_expr)
    return (list_expr_all[0][0] & list_expr_all[0][1] & list_expr_all[0][2] & list_expr_all[0][3] & list_expr_all[0][4]
        & list_expr_all[1][0] & list_expr_all[1][1] & list_expr_all[1][2] & list_expr_all[1][3] & list_expr_all[1][4])


def count_event_mtg_expr():
    """ Generate query expression for counting how many mortgage loan event for a record (up to 5)
    """
    return (F.when(F.col('mortgage1_limit') != 0, 1).otherwise(0)
            + F.when(F.col('mortgage2_limit') != 0, 1).otherwise(0)
            + F.when(F.col('mortgage3_limit') != 0, 1).otherwise(0)
            + F.when(F.col('mortgage4_limit') != 0, 1).otherwise(0)
            + F.when(F.col('mortgage5_limit') != 0, 1).otherwise(0))


def count_event_heq_expr():
    """ Generate query expression for counting how many home equity loan event for a record (up to 5)
    """
    return (F.when(F.col('homeequity1_limit') != 0, 1).otherwise(0)
            + F.when(F.col('homeequity2_limit') != 0, 1).otherwise(0)
            + F.when(F.col('homeequity3_limit') != 0, 1).otherwise(0)
            + F.when(F.col('homeequity4_limit') != 0, 1).otherwise(0)
            + F.when(F.col('homeequity5_limit') != 0, 1).otherwise(0))


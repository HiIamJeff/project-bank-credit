
import polars as pl
import warnings
# suppress warnings from uszipcode
warnings.filterwarnings("ignore", message="Using slow pure-python SequenceMatcher")

# from uszipcode import SearchEngine
from general_utils import get_zipcode_dictionary


def monthly_data_transformation(df):
    """
    Process monthly data
    return new data for analysis and future steps (ML pipeline)
    """
    # join demographic table
    demo_data_dir = 'data/source/demographic_data/zip9_demographics_coded_pv.csv'
    df_demo = pl.scan_csv(demo_data_dir, dtypes=get_schema_demo())

    # join
    df_merged = df.join(df_demo.drop('zip5'), left_on="zip9_code", right_on='zip9_code', how="inner")

    # imputation
    df_merged = impute_monthly_data(df_merged)

    df_merged = (df_merged.with_columns(
        [
            pl.col('zip5').map_dict(get_zipcode_dictionary()).alias("geo_state_major_city"),
        ]
    ).with_columns(
        [
            pl.col('geo_state_major_city').list.get(0).alias('state'),
            pl.col('geo_state_major_city').list.get(1).alias('major_city'),
            count_event_mtg_expr().alias('count_mtg_event'),
            count_event_mtg_expr().alias('count_heq_event'),
            generate_mtg_heq_valid_bool_expr().alias('mtg_heq_valid_flag')
        ]
    ).drop('geo_state_major_city').select(pl.col('*')))
    return df_merged


def get_schema_demo():
    dict_schema = {
        'zip5': pl.Utf8,
        'zip9_code': pl.Int32,
        'age': pl.Float32,
        'household_count': pl.Int32,
        'person_count': pl.Int32,
        'homebuyers': pl.Int32,
        'first_homebuyers': pl.Int32,
    }
    return dict_schema


def impute_monthly_data(df):
    """ Basic imputation for monthly data (excluding specific columns)
    """
    list_num_col = [c for c, d in df.schema.items() if d in [pl.Float32, pl.Int32]]
    list_str_col = [c for c, d in df.schema.items() if d in [pl.Utf8]]

    list_num_exception = ['bankcard_util', 'total_revolving_util', 'mortgage1_loan_to_value']
    list_num_col = [c for c in list_num_col if c not in list_num_exception]

    df = df.with_columns([
        pl.when(pl.col(list_num_col).is_null())
        .then(0)
        .otherwise(pl.col(list_num_col))
        .keep_name()
    ])
    return df


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
            expr = ((pl.col(col_list[0]) != 0)
                    & (pl.col(col_list[1]) != 0)
                    & (pl.col(col_list[2]) != 0)) | ((pl.col(col_list[0]) == 0)
                                                     & (pl.col(col_list[1]) == 0)
                                                     & (pl.col(col_list[2]) == 0))

            list_expr.append(expr)
        list_expr_all.append(list_expr)
    return (list_expr_all[0][0] & list_expr_all[0][1] & list_expr_all[0][2] & list_expr_all[0][3] & list_expr_all[0][4]
        & list_expr_all[1][0] & list_expr_all[1][1] & list_expr_all[1][2] & list_expr_all[1][3] & list_expr_all[1][4])


def count_event_mtg_expr():
    """ Generate query expression for counting how many mortgage loan event for a record (up to 5)
    """
    return ((pl.when(pl.col('mortgage1_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('mortgage2_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('mortgage3_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('mortgage4_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('mortgage5_limit') != 0).then(1).otherwise(0)))


def count_event_heq_expr():
    """ Generate query expression for counting how many mortgage loan event for a record (up to 5)
    """
    return ((pl.when(pl.col('homeequity1_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('homeequity2_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('homeequity3_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('homeequity4_limit') != 0).then(1).otherwise(0))
            + (pl.when(pl.col('homeequity5_limit') != 0).then(1).otherwise(0)))

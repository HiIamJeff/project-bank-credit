import pytest
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

from polars_pipeline.monthly_transformation_script import monthly_data_transformation
from polars_pipeline.ingestion_script import get_schema_monthly
from polars_pipeline.monthly_transformation_script import get_schema_demo

WORKING_DIR = Path('').resolve()
PATH_TEST_DIR = Path(WORKING_DIR, 'tests/test_data')


def test_polars_monthly_data_transformation() -> None:
    """ Test the whole polar monthly data transformation
    """
    sample_df = (pl.scan_csv(Path(PATH_TEST_DIR, 'zip9_coded_201904_pv_test.csv').as_posix(),
                             dtypes=get_schema_monthly())
                 .filter(pl.col('zip9_code').is_in([703023, 627746, 3779203, 802280, 3471270, 1478042])))

    demo_data_test_dir = Path(PATH_TEST_DIR, 'zip9_demographics_coded_pv_test.csv').as_posix()

    # polars tends to convert numeric columns to be 64-bit when reading CSVs
    expected_df = pl.scan_csv(Path(PATH_TEST_DIR, '201904_monthly_processed_data_expected.csv').as_posix(),
                              dtypes=get_schema_monthly() | get_schema_demo() | {'count_mtg_event': pl.Int32,
                                                                                 'count_heq_event': pl.Int32})

    assert_frame_equal(monthly_data_transformation(sample_df, demo_data_dir=demo_data_test_dir),
                       expected_df)
    return

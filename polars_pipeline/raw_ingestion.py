
import os
import sys
from pathlib import Path
import argparse

from polars_pipeline.ingestion_script import monthly_data_ingestion


parser = argparse.ArgumentParser()
parser.add_argument('--input_year', required=True, type=lambda x: int(x))
parser.add_argument('--input_month', default=0, required=False,
                    type=lambda x: int(x) if int(x) in range(1, 13) else False)
parser.add_argument('--test_run', default=False, required=False,
                    type=lambda x: (str(x).lower() == 'true'))

args = parser.parse_args()
if args.input_month is False:
    raise parser.error('Invalid input_month')


WORKING_DIR = Path('').resolve()
RAW_INPUT_DIR = 'data/source'

month_range = range(4, 13) if args.input_month == 0 else range(args.input_month, args.input_month + 1)

# Ingestion
for month in month_range:
    print(f'processing data for {args.input_year}/{month}...')

    input_file = Path(WORKING_DIR, RAW_INPUT_DIR, f'zip9_coded_{args.input_year}{month:02d}_pv.csv').as_posix()
    output_path = Path(WORKING_DIR, f'data/processed_polars/').as_posix()

    try:
        monthly_data_ingestion(input_path=input_file, output_path=output_path,
                               input_year=args.input_year, month=month, test_run=args.test_run)
    except Exception as e:
        print('-- ERROR --')
        print(e)


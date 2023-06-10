# THE SCRIPT ITSELF (cli.examples.sh) SHOULD NOT BE RAN DIRECTLY AS WHOLE

## spark
# For standalone mode (driver/executor on a single machine) Run it after Spark master/worker has been started
# In working directory

# Run the whole year
spark-submit --master <spark-master-url> --name applicationstream_sample \
--num-executors 2 --executor-memory 2g \
--driver-memory 2g raw_ingestion.py --input_year=2019 --ingestion_mode=overwrite

# Run the specific month
spark-submit --master <spark-master-url> --name applicationstream_sample \
--num-executors 2 --executor-memory 2g \
--driver-memory 2g raw_ingestion.py --input_year=2019 --ingestion_mode=overwrite --input_month 4

# Run tests (with limited records)
spark-submit --master <spark-master-url> --name applicationstream_sample \
--num-executors 2 --executor-memory 2g \
--driver-memory 2g raw_ingestion.py --input_year=2019 --ingestion_mode=overwrite --input_month 4 --test_run True

# Run streamlit app (please be sure launch it at the project root directory)
streamlit run streamlit_app/app.py


## polars
# Run the whole year
python polars_pipeline/raw_ingestion.py --input_year=2019

# Run the specific month
python polars_pipeline/raw_ingestion.py --input_year=2019 --input_month 4

# Run tests (limited records)
python polars_pipeline/raw_ingestion.py --input_year=2019 --input_month 4 --test_run True


# project-bank-credit

![tests](https://github.com/HiIamJeff/project-bank-credit/actions/workflows/tests.yml/badge.svg)

Showcase for modern data engineering with Spark and Polars

### Purpose
The project is a personal practice and showcase for how pipelines built with [Apache Spark](https://spark.apache.org/) or [Polars](https://www.pola.rs/) can do ETL/ELT. The applications could be run locally or hosted on public cloud services with some modifications. With the appropriate settings, the pipelines could also handle data volume from a few GBs to TB-level data (Spark, specifically). 

### Content
A few things that the pipelines cover:
- Accepting monthly data from a local filesystem (or a data lake on a cloud)
- Cleaning and transforming monthly data with business logic and geographic information 
- Outputting the results in Parquet for further purposes (analysis or model training)
- Generating a monthly report in CSVs at the end of each monthly job
- Firing a self-service report application (built by [Streamlit](https://streamlit.io/)) that shows a choropleth map with various measures and an interactive dataframe viewer
  - For more details, please check `README.md` in `streamlit_app/`
- Being able to run jobs for monthly process, full migration (by years), or test mode (with limited records)
- The repo is set up with GitHub Action for basic CI (testing the whole transformation with sample data). The user can run `pytest` to do the basic test locally as well (The Spark pipeline one is still in development)

### Data
The project is built with US bank credit data.
1.  The main one is the monthly homebuyer data at zip code 9 (zip9) level, containing information about US consumers' credit activities relevant to home buying. This data covers info from April to September 2019, and it is split into several CSVs by month.
2.  The second is the demographic data with basic information about consumers at the zip9 level.

The total data size is about 10 GB. Nonetheless, the pipelines are designed for an ongoing monthly process, and they should be able to handle 10x-100x of size with some tweaks in settings.
> All datasets are from [Brown Datathon 2020 event]((https://dsi.brown.edu/news/2020-03-04/brown-datathon)) and the actual zip9 value has been coded. The datasets are only for educational purposes and are not intended for other purposes.   

### Instruction
For anyone interested in trying this repo themselves, [the Google Drive folder](https://drive.google.com/drive/u/0/folders/1D-DVKXOFfkN1QkwV8PZ2h83AL8wA6Rov) has sample data. It is the same data but with roughly 0.05% of the records (zip codes only within New York City). Please place them following the Folder Structure section. Though the small data volume may not fully utilize these frameworks, it is still a good starting ground and can be replicated with other big data projects.

Please use `requirement.txt` to create a virtual environment with Python 3.9 and set up Spark on your machine. For the Spark application, it is best to run this in Standalone mode (requiring setting up Spark driver/worker and submitting jobs to launch the application). To launch it, please check examples in `cli_examples.sh` (as well as for the Streamlit app)

> p.s.1 Setting Spark on any environment could be a fuzzy task. You can refer to this [simple guide](https://www.sundog-education.com/spark-python/) or any other resources to set it up correctly.


### Folder Structure
```bash
.
├── LICENSE
├── README.md
├── cli_examples.sh
├── data
│   ├── processed
│   ├── processed_polars
│   │   └── report
│   └── source
│       ├── demographic_data
│       │   └── zip9_demographics_coded_pv.csv
│       ├── zip9_coded_201904_pv.csv
│       ├── zip9_coded_201905_pv.csv
│       ├── zip9_coded_201906_pv.csv
│       ├── zip9_coded_201907_pv.csv
│       ├── zip9_coded_201908_pv.csv
│       └── zip9_coded_201909_pv.csv
├── general_utils.py
├── polars_pipeline
│   ├── README.md
│   ├── ingestion_script.py
│   ├── monthly_transformation_script.py
│   └── raw_ingestion.py
├── requirements.txt
├── spark_pipeline
│   ├── README.md
│   ├── ingestion_script.py
│   ├── monthly_transformation_script.py
│   └── raw_ingestion.py
├── streamlit_app
│   ├── README.md
│   ├── app.py
│   ├── list_zipcode_nyc.pkl
│   ├── streamlit_script.py
│   └── zipcode_2020_shape_simplified.geojson
├── tests
│   ├── test_data
│   │   ├── 201904_monthly_processed_data_expected.csv
│   │   ├── zip9_coded_201904_pv_test.csv
│   │   └── zip9_demographics_coded_pv_test.csv
│   └── test_polars.py

```

#### Future development
Some ideas that can expand the project furthermore:
- Setting up an orchestration tool (e.g., Airflow, Prefect) to make this process a monthly schedule task
- Modifying the batch job to be a streaming process (Structured Streaming) to run every time the new/updated data comes in
- Setting up another process to re-train machine learning models with the latest data
- Setting up JDBC drivers to read/write data from/to external database systems (e.g., Postgres)
- Extending the project by exporting wide tables in the final stage and connecting those with BI platforms on cloud services (e.g., BigQuery with Looker)

## Contacts
Please let me know if you have any thoughts on this project.

- Linkedin: https://www.linkedin.com/in/jefflu-chia-ching-lu/
- Email: cl3883@columbia.edu

Jeff


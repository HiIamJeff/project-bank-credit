# project-bank-credit
Showcase for modern data engineering with Spark and Polars

### [UNDER DEVELOPMENT for the Polars part]

### Purpose
The project is a personal practice and showcase for how pipelines built with [Apache Spark](https://spark.apache.org/) or [Polars](https://www.pola.rs/) can do ETL/ELT. The applications could be run locally or hosted on public cloud services with some modifications. With the appropriate settings, the pipelines could also handle data volume from a few GBs to TB-level data (Spark, specifically). 

### Content
Here are a few things that the pipelines cover:
- Accepting monthly data from a local filesystem (or a data lake on cloud)
- Cleaning and transforming monthly data with business logic and geographic information 
- Outputting the results in Parquet for further purposes (analysis or model training)
- Generating monthly-aggregated reports in CSVs at the end of a job run
- Being able to run jobs for monthly process, full migration (by years), or test mode (with limited records)

### Data
The project is built with US bank credit data.
1.  The main one is the monthly homebuyer data at zip code 9 (zip9) level, containing information about US consumers' credit activities relevant to home buying. This data covers info from April to September 2019, and it is split into several CSVs by month.
2.  The second is the demographic data with basic information about consumers at the zip9 level.

The total data size is about 10 GB. Nonetheless, the pipelines are designed for an on-going monthly process, and they should be able to handle 10x-100x of size with some tweaks in settings.
> All datasets are from Brown Datathon 2020 event and the actual zip9 value has been coded. The datasets are only for educational purposes and are not intended for other purposes.   

### Instruction
For anyone who is interested in trying this repo themselves, [the Google Drive folder](https://drive.google.com/drive/u/0/folders/1D-DVKXOFfkN1QkwV8PZ2h83AL8wA6Rov) has sample data. It is the same data but with only 1% of the records. Please place them following the Folder Structure section. Though the small data volume may not fully utilize these frameworks, it is still a good starting ground and can be replicated with other big data project.

Please use requirement.txt to create a virtual environment with Python 3.9+ and set up Spark on your machine. For actual Spark application, it is best to run this with Standalone mode (setting up Spark driver/worker, submitting jobs to launch the application)

> p.s. Setting Spark on any environments could be a fuzzy task. You can refer to this [simple guide](https://www.sundog-education.com/spark-python/) or any other resources to set up correctly. You can also check examples in `cli_examples.sh` for your own testing


### Folder Structure
```bash
.
├── LICENSE
├── README.md
├── cli_examples.sh
├── data
│   ├── processed
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
├── ingestion_script.py
├── monthly_transformation_script.py
├── raw_ingestion.py
├── requirements.txt
```
#### Future development
Some ideas that can expand the project furthermore:
- Setting up an orchestration tool (e.g., Airflow, Prefect) to make this process a monthly schedule task
- Modifying the batch job to be a streaming process (Structured Streaming) to run every time the new/updated data comes in
- Setting up another process to train machine learning models with the latest data
- Setting up JDBC drivers to read/write data from/to external database systems (e.g., Postgres)

## Contacts
Please let me know if you have any thought on this project.

- Linkedin: https://www.linkedin.com/in/jefflu-chia-ching-lu/
- Email: cl3883@columbia.edu

Jeff


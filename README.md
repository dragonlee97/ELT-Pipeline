# Introduction
This project holds a standard ETL pipeline orchestrated as an Airflow DAG. It consists of
- Extract fetching data from airtable and writing files into S3 buckets
- Load: Ingesting raw data from S3 bucket into redshift tables 
- Transform: Reshaping data into final table


# Environments:
- Airflow version: 2.5.3
- Python version: 3.9

For local runing, you can launch airflow in docker, which is already set up in docker-compose.yaml and Dockerfile. To do this:

0. You need docker and docker-compose to be installed; Docker needs to have access to enough memory (8 GB is enough)
1. Prepare local directories and initalize airflow settings (It would take a few minutes):
```bash
make prepare
```
2. Launch airflow in docker (It would take a dozens of seconds):
```bash
make airflow-run
```
3. Go to airflow Web UI at <localhost:8080> with default username and password:
```bash
Username: airflow
Password: airflow
```

4. You can than trigger/monitor the pipeline over the UI

5. After the pipeline is finished, you can shut down the container with:
```bash
make stop-airflow
```

# Connections & Parameters
This pipeline uses Airtable & AWS (S3,redshift). You need to properly set up the credentials in order to run the DAG
## AWS
You need to set up the following two connections from the Airflow UI
- aws_default: Using your `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY`
- redshift_default: Using the correct `HOST`, `PORT`, `USER`, `PASSWORD`, `DB` 
## Airtable
- `AIRTABLE_BASE_ID` is already saved as an environment variable in the docker-compose.yaml
- `AIRTABLE_API_KEY` is passed as an Airflow variable, you need to create it from Airflow UI as well
## Params.json
It's a json file where saved the parameters of the dag. Such as: start_date, schedule, resource name used in the dag



# Data Engineering Good Practices
1. Idempotent: Running the pipeline multiples times has always the same results each time
2. Merge (Upsert) when ingests new data to avoid duplicates
3. Add sensor and quality check at every step: `dbt build` is a good tool to prevent from populating bad data to downstream
4. Create/Delete/Update schema of a table can be done using cloud service CLI 







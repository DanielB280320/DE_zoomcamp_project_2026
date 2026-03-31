# DE Zoomcamp 2026 Project | US Housing Market Analysis

## Problem Description 

The U.S. housing market generates enormous amounts of data every week across hundreds regions and all 50 states. For buyers, sellers, investors, and real estate professionals, there is no simple way to monitor how the market is evolving across different geographies and time periods without building the infrastructure to process it themselves. However, this data is published by Redfin as raw, compressed TSV files that are updated weekly and require significant technical work to download, clean, and analyze. 

This gap leads to a concrete problem: housing market decisions are made without visibility into trends. A buyer considering San Francisco vs. San Jose has no easy way to compare how median sale prices or days on market have shifted over the past 9 years in each city. An investor evaluating states can't quickly identify that Massachusetts consistently leads in average sale price while Hawaii dominates in price per square foot. A seller in Phoenix doesn't know whether active listings in their metro are rising or shrinking relative to comparable markets like Atlanta or Chicago.

### What this project solves

This project addresses that problem by building an automated pipeline that ingests, transforms, and models Redfin's weekly housing market data, and exposes it through an interactive analytical dashboard with two levels of geographic analysis:

- Metropolitan/State level — aggregates those same metrics across all regions within each state, allowing macro comparisons of market health, affordability, and competition across the entire country

- Metropolitan/Region level — ranks the top regions by average sale price, price per square foot, homes sold, active listings, and days on market, with 9 years of weekly trend lines (2017–2026) and filters by State and Region name

Together, these views turn a fragmented, hard-to-use public dataset into a self-updating, drill-down market intelligence tool — answering in seconds questions that would otherwise require hours of manual data work.

---

## Cloud

For the develop of this project is used as Cloud Service Provider the Google Cloud Platform (GCP), so previously you must have created a GCP project, a service account and have generated a key service account to get access to all the resources in your Google Cloud. 

To manage all the Cloud infrastructure as code Terraform is used also for documentation and version control; The resources created were the project bucket within the GCS datalake where all the raw data is stored in .parquet format and also partitioned leveraging the Spark distribuited processing; Also was created a dataset within the Bigquery data warehouse where all the fetched data from the datalake is stored for further processing

---

## Data Ingestion: Batch Processing / Workflow orchestration

The pipeline was designed to process batch data based in the dataset used for the develop of the project; The redfin US Housing Market Dataset is generated in a weekly basis every wednesday with the real state data for the US market of the previous week.

The pipeline works completely in automatic way and all the jobs/tasks are fully orchestrated with a multi step DAG through Airflow as orchestrator:

- **Fetching data task:** The first task is a Spark Job making use of the GCS Connector for Spark Hadoop to create and Spark Session which is running is an local Spark cluster. This job process all the raw data extracted from the source server (a generated link address in the redfin page where the data is accesible and updated every week); The data is downloaded and stored locally in a temporary directory from where Spark read the data in a .tsv format (similarly to .csv format but tab separate values) make an initial schema definition to the dataset usefull for further partitioning and clustering and finally write the dataset to the Cloud external path in a .parquet format and also partitioned leveraging the Spark distribuited processing for large workloads (the dataset from 2017 up to date contains about 5.6+ Millions rows).

- **GCS to Bq task:** When all raw data is already stored in the datalake another Spark Job using the GCS and Bigquery connectors (at the same time when creating the Spark Session) for Spark Hadoop read all the partitioned .parquet files. Before loading the data to Bigquery its recommended to define an existing bucket as temporal bucket or create a new one for Bigquery to stored the data temporaly due in case the BQ load fails, the raw file is still safe in GCS. Finally the data is wrote to the data warehouse partitioned by the period_begin field (date column) and clustered by the region_name field (city/region name column). 

- **dbt model task:** The dbt model task in the DAG is in charge of cleaning, transform, modeling and enrich the data through each of the defined stages in the model: Staging, Intermediate and Marts

    # Job dependencies
    fetching_data_job >> gcs_to_bq_job >> dbt_model_job

---

## Data warehouse: 

As mentioned early, before storing the data into the data warehouse the consolidated table is partitioned and clustered in the following way: 

- Partitioned by **period_begin** is critical because the dataset spans nearly a decade of weekly housing market data (2017–2026). Since queries and the dashboard filters operate on a specific time range, partitioning by date allows BigQuery to skip irrelevant partitions entirely and scan only the time window needed, dramatically reducing query costs and execution time.

- Clustering by **region_name** further optimizes performance within each partition. Since the dashboard heavily filters and ranks data by region/city (comparing 
metros like San Francisco, Atlanta, or Phoenix) BigQuery can co-locate rows from the same region on the same storage blocks. This means that queries filtering or  grouping by region scan significantly less data, making the ranking tables and trend line charts on reports faster and more cost efficient to compute.

---

## Transformations:

The entire pipeline's transformations were performed through a structured dbt model where each layer/stage in the model is well defined:

- **Staging** (`stg_us_housing_data.sql`): The first layer reads directly from 
    the raw BigQuery table defined in `bq_sources.yml`. It applies basic cleaning 
    like renaming columns and casting data types to produce a standardized and consistent 
    base model that all downstream models depend on.

- **Intermediate** (`int_us_housing_data.sql`): This layer applies business logic 
    transformations on top of the staging model such as deriving calculated fields, 
    joining related datasets/seeds, deduplicating, filtering null values and preparing the data for dimensional modeling.

- **Marts** — Dimensional Models: This layer implements a **star schema** structure 
    with dedicated dimension and fact tables:
    - `dim_region.sql` — contains the region attributes
    - `dim_region_type.sql` — classifies regions by type (county or metro)
    - `dim_state.sql` — contains state-level attributes enriched and using fields of the us_states seed (state name, region and division)
    - `fact_housing_data.sql` — the central fact table containing all housing 
      market records and quantitative fields/columns like metrics that later can be joined to the dimension tables

- **Marts** — Reporting Models: The final layer pre-aggregates the fact and 
    dimension tables into ready-to-query reporting table that directly power 
    the dashboard:
    - focused weekly aggregations model directly used for the multiple charts and tables in the dashboard.

![dbt_model_lineage](/pictures/dbt_model_lineage.png)

This layered approach ensures **data quality is enforced early**, **logic is centralized** in one place, and the **dashboard always queries pre-aggregated, optimized models** rather than hitting the raw data directly.

---

## Dashboard:

### US Housing Market State (Metropolitan)
![report_state_slide](pictures/report_state_slide.png)

### US Housing Market Region (Metropolitan)
![report_region_slide](pictures/report_region_slide.png)

### Filtering By Region/State
![report_region_slide_filtered](pictures/report_region_slide_filtered.png)

### Access to the report
https://lookerstudio.google.com/s/kMEl555lQmI

---

## Reproducibility: Requirements

Besides the pipeline use Spark - for the Spark Jobs - is not required or necessary to install Scala or Spark in your machine because all these requirements are previously configured in the docker-compose.yaml which is created using a custom dockerfile with an Airflow docker image where all the programs and dependencies are setting up to be installed when building and starting the Airflow containers; Also the Spark Hadoop connectors for GCS and Bigquery are already stored within the project and mounted in a volume within the container, so there is no needed to install any additional drivers.

### Required: 
- Docker-compose Installed
- Terraform Installed and also configured the Cloud Infrastructure
- Environment variables file (.env)

Run the pipeline with following steps: 

Clone the pipeline repository in your host machine in a directory: 

    clone remote <repo-name>

Create a new file '.env' with the following defined variables: 

    # Airflow environment variables
    AIRFLOW_UID= 50000

    # Cloud environment variables
    GCS_CREDENTIALS= /opt/airflow/gcs_credentials/credentials/service_account_creds.json
    PROJECT_ID= your-project-id
    REGION= your-bucket-region (eg. US, EU)
    BUCKET_NAME= your-bucket-name
    DATASET_NAME= your-dataset-name

The pipeline is parametarized to use the previous variables defined.

Within the project directory where your docker-compose.yaml is located execute the following to build the Airflow containers: 

    # Build and run all the Airflow containers
    docker compose up 

    # Build and run all the Airflow containers in detached mode
    docker compose up -d

Wait until all the containers are running and healthy, you can validate the status: 

    # docker compose ps

When the airflow-apiserver container is up and healthy open the Airflow UI in the port 8080:

    http://localhost:8080

Go to the dags section and select the pipeline 'us_housing_data_pipeline'

Next toogle the switch next to the pipeline name and select 'Trigger' in the up-right corner:

![airflow_ui_2](pictures/airflow_ui_2.png)

Within the new open window select 'Unique execution' and finally select 'Trigger' again. After that the pipeline is already executing.










## IaC: Installing Terraform

To manage all the Cloud infrastructure as code Terraform is used, so as first we need to install it: 

    # Download the binary
    wget https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip

    # Unzip it
    unzip terraform_1.7.5_linux_amd64.zip

    # If you dont have unzip also you need to install it
    sudo apt-get install unzip

    # Move to PATH
    sudo mv terraform /usr/local/bin/

    # Clean up
    rm terraform_1.7.5_linux_amd64.zip

To verify the installation

    terraform -v


## IaC: Initializing Terraform

Terraform must authenticate to Google Cloud to create infrastructure, so you need to have installed locally the gcloud CLI.

In your terminal, use the gcloud CLI to set up your Application Default Credentials.

    gcloud auth application-default login

Your browser will open and prompt you to log in to your Google Cloud account. After successful authentication, your terminal will display the path where the gcloud CLI saved your credentials.

    Credentials saved to file: [/Users/USER/.config/gcloud/application_default_credentials.json]

These credentials will be used by any library that requests Application Default Credentials (ADC).

Initializing

    terraform init
    terraform fmt
    terraform validate
    terraform plan
    terraform apply

Validate if our infrastructure was created succesfully

    gcloud storage ls

## Installing Spark: 

For use Spark first at all we need to install Java version 17 or 21 which is requiered.

Also we need to download the gcs connector to connect our local spark cluster to the gcs datalake; The jar connector can be downloaded from the gcs remote location as follow:

    # Spark gsc connector
    - gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar 

    reference: 
    https://docs.cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage?hl=es-419

    Download from gcs remote location to our machine: 
    - gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar


Check spark version and Scala version: 
    spark-shell --version
    # or
    ls $SPARK_HOME/jars/ | grep scala-library
    

Spark bigquery connector: 

    #References: 
    https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file

## Installing dbt: 

To installing dbt with our bigquery adapter we use: 

    # Install dbt
    uv run pip install dbt-bigquery

    # Check version installed
    uv run dbt --version

    # Validate the connection to the datasource
    uv run dbt debug







## Docker addtional commands: 

    # Add user to docker group
    sudo usermod -aG docker $USER

    # Verify group membership
    groups                                                   
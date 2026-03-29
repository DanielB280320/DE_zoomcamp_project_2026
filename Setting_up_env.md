# Setting up environment: 

## Setting up cloud provider: 

For the develop of this project we use as a Cloud Service Provider (CSP) Google Cloud Platform so first at all, you must previously have created a GCP project, a service account and have generated a key service account to get access to all the resources on the cloud. 

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
import datetime
from urllib import request
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': YESTERDAY,
}

# Function to download the mobility data.
def download_file():
    # Define the remote file to retrieve
    remote_url = 'https://storage.googleapis.com/dsgo-2021-de-lab/applemobilitytrends-2021-07-22.csv'
    # Define the local filename to save data
    local_file = '/home/airflow/gcs/data/raw-mobility-data.csv'
    # This file location is automatically synced from Cloud Composer nodes to the Composer data bucket in
    # Google Cloud Storage.
    request.urlretrieve(remote_url, local_file)


with airflow.DAG(
        'dsgo_sample_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval=None) as dag:
    # Don't schedule - manual run.

    # Run the download function.
    download = PythonOperator(
        task_id='download',
        python_callable=download_file,
    )

    # Load the raw data into BigQuery.
    load_data_to_bq = BashOperator(
        task_id='load_data_to_bq',
        # Executing 'bq' command requires Google Cloud SDK which comes
        # preinstalled in Cloud Composer.
        bash_command='''bq load --autodetect \\
        --replace mobility_data.raw_data \\
        gs://composer-data-bucket-path/raw-mobility-data.csv''')
    # Adjust this gcs path by finding your Composer GCS bucket and looking for
    # the data folder.
    # The table name will work so long as you've used the dataset name 'mobility_data'.

    unpivot = BashOperator(
        task_id='unpivot',
        # Unpivot the data and create a table with the results.
        # Adjust the project name in both table names.
        bash_command='''bq query --nouse_legacy_sql \
        'CREATE OR REPLACE TABLE `your-project-name.mobility_data.unpivoted_data` as (
                SELECT a.geo_type, region, transportation_type, unpivotted.*
                FROM `your-project-name.mobility_data.raw_data` a
                , UNNEST(fhoffa.x.cast_kv_array_to_date_float(fhoffa.x.unpivot(a, "_202"), "_%Y_%m_%d")) unpivotted
                )'
        ''')

    export = BashOperator(
        task_id='export',
        # Export the processed data.
        # Adjust the bucket name so this will run.
        # The table name should work so long as you've used the dataset name 'mobility_data'.
        bash_command='''bq extract 'mobility_data.unpivoted_data' \
                        gs://export-bucket-name/apple-mobility-export.csv
            ''')

    # Set task dependencies.
    download>>load_data_to_bq>>unpivot>>export


# Import packages
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
   
# Define default arguments
default_args = {
    'owner': 'Patrícia Nati',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# Define dag variables
project_id = 'playground-s-11-b1c8573c'
staging_dataset = 'DWH_STAGING'
dwh_dataset = 'DWH'
gs_bucket = 'playground-s-11-b1c8573c-data'
FILE_NAME = 'processed.csv'

# Define dag
dag = DAG('cloud-data-lake-pipeline',
          start_date=datetime.now(),
          schedule_interval='0 * * * *', #'@once',
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)

sens_object_create = GoogleCloudStorageObjectSensor(
        task_id='sens_object_create',
        bucket= gs_bucket,
        object=f'Output_path/{FILE_NAME}',
        google_cloud_conn_id='google_cloud_default'
    )

# Load data from GCS to BQ
load_vendas_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_vendas',
    bucket = gs_bucket,
    #source_objects = [f'Output_path/{FILE_NAME}'],
    source_objects = f'Output_path/{FILE_NAME}',
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.vendas_staging',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    field_delimiter=',',
    skip_leading_rows = 1,
    schema_fields=[
        {
            "name": "extract_date",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Data extração"
        },
        {
            "name": "ID_MARCA",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "ID Marca produto"
        },
        {
            "name": "MARCA",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "Marca"
        },
        {
            "name": "ID_LINHA",
            "type": "STRING",
            "mode": "NULLABLE",
        "description": "ID da linha"
        },
        {
            "name": "LINHA",
            "type": "STRING",
            "mode": "NULLABLE",
        "description": "linha"
        },
        {
            "name": "DATA_VENDA",
            "type": "DATE",
            "mode": "NULLABLE",
            "description": "Data da venda"
        },
        {
            "name": "QTD_VENDA",
            "type": "INTEGER",
            "mode": "NULLABLE",
            "description": "Quantidade de venda"
        }
        ]

)

# Check loaded data not null
check_vendas_demo = BigQueryCheckOperator(
    task_id = 'check_vendas_demo',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.vendas_staging`'

)

loaded_data_to_staging = DummyOperator(
    task_id = 'loaded_data_to_staging'
)

# Transform, load, and check fact data
create_vendas_ano_mes = BigQueryOperator(
    task_id = 'create_vendas_ano_mes',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/vendas_ano_mes.sql'
)

check_vendas_ano_mes = BigQueryCheckOperator(
    task_id = 'check_vendas_ano_mes',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) FROM `{project_id}.{dwh_dataset}.vendas_ano_mes`'
)

# Create remaining dimensions data
create_marca_linha = BigQueryOperator(
    task_id = 'create_marca_linha',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/vendas_marca_linha.sql'
)
check_marca_linha = BigQueryCheckOperator(
    task_id = 'check_marca_linha',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) FROM `{project_id}.{dwh_dataset}.vendas_marca_linha`'
)

# Create remaining dimensions data
create_marca_ano_mes = BigQueryOperator(
    task_id = 'create_marca_ano_mes',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/vendas_marca_ano_mes.sql'
)
check_marca_ano_mes = BigQueryCheckOperator(
    task_id = 'check_marca_ano_mes',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) FROM `{project_id}.{dwh_dataset}.vendas_marca_ano_mes`'
)

create_linha_ano_mes = BigQueryOperator(
    task_id = 'create_linha_ano_mes',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/vendas_linha_ano_mes.sql'
)
check_linha_ano_mes = BigQueryCheckOperator(
    task_id = 'check_linha_ano_mes',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) FROM `{project_id}.{dwh_dataset}.vendas_linha_ano_mes`'
)

create_linha_dia_ano_mes = BigQueryOperator(
    task_id = 'create_linha_dia_ano_mes',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/vendas_linha_dia_ano_mes.sql'
)
check_linha_dia_ano_mes = BigQueryCheckOperator(
    task_id = 'check_linha_dia_ano_mes',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) FROM `{project_id}.{dwh_dataset}.vendas_linha_dia_ano_mes'
)

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

# Define task dependencies
#dag >> start_pipeline >> [load_us_cities_demo, load_airports, load_weather, load_immigration_data]
dag >> start_pipeline >> sens_object_create >>load_vendas_demo >> check_vendas_demo >> loaded_data_to_staging
loaded_data_to_staging >> [create_vendas_ano_mes, create_marca_linha, create_marca_ano_mes,create_linha_dia_ano_mes] 
create_vendas_ano_mes>> check_vendas_ano_mes
create_marca_linha >> check_marca_linha
create_marca_ano_mes >> check_marca_ano_mes
create_linha_dia_ano_mes >> check_linha_dia_ano_mes
#loaded_data_to_staging >> finish_pipeline

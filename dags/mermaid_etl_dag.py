from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Dict
import pandas as pd
import requests
from airflow.utils.log.logging_mixin import LoggingMixin

from utils.mermaid_utils import (
    fetch_mermaid_data,
    transform_beltfish_data,
    transform_coral_data,
    transform_quadrat_data,
    load_to_database,
    FISH_SURVEY_ENDPOINT,
    CORAL_SURVEY_ENDPOINT,
    PHOTO_QUADRAT_ENDPOINT
)
from config.mermaid_config import DatabaseConfig

def get_rare_project_ids() -> list:
    """Fetch project IDs from Mermaid API with Rare tag"""
    url = "https://api.datamermaid.org/v1/projects/?showall=true&tags=Rare"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Extract project IDs from the response
        project_ids = [project["id"] for project in data["results"]]
        return project_ids
    except Exception as e:
        raise Exception(f"Failed to fetch project IDs: {str(e)}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_fish_data(**context):
    """Extract fish survey data from Mermaid API"""
    logger = LoggingMixin().log
    project_ids = context['dag_run'].conf.get('project_ids', get_rare_project_ids())
    
    logger.info(f"Fetching fish survey data for project IDs: {project_ids}")
    
    dfs = []
    failed_projects = []
    
    for project_id in project_ids:
        logger.info(f"Fetching fish data for project ID: {project_id}")
        try:
            df = fetch_mermaid_data(project_id, FISH_SURVEY_ENDPOINT)
            if not df.empty:
                dfs.append(df)
                logger.info(f"Successfully fetched fish data for project ID: {project_id}")
        except Exception as e:
            logger.warning(f"Failed to fetch fish data for project ID {project_id}: {str(e)}")
            failed_projects.append(project_id)
            continue
    
    if not dfs:
        raise Exception("No fish survey data could be fetched from any project")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    context['task_instance'].xcom_push(key='fish_data', value=combined_df.to_dict())
    context['task_instance'].xcom_push(key='fish_data_shape', value=combined_df.shape)
    context['task_instance'].xcom_push(key='failed_projects', value=failed_projects)
    return "Fish data extraction successful"

def extract_coral_data(**context):
    """Extract coral survey data from Mermaid API"""
    logger = LoggingMixin().log
    project_ids = context['dag_run'].conf.get('project_ids', get_rare_project_ids())
    
    logger.info(f"Fetching coral survey data for project IDs: {project_ids}")
    
    dfs = []
    failed_projects = []
    
    for project_id in project_ids:
        logger.info(f"Fetching coral data for project ID: {project_id}")
        try:
            df = fetch_mermaid_data(project_id, CORAL_SURVEY_ENDPOINT)
            if not df.empty:
                dfs.append(df)
                logger.info(f"Successfully fetched coral data for project ID: {project_id}")
        except Exception as e:
            logger.warning(f"Failed to fetch coral data for project ID {project_id}: {str(e)}")
            failed_projects.append(project_id)
            continue
    
    if not dfs:
        raise Exception("No coral survey data could be fetched from any project")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    context['task_instance'].xcom_push(key='coral_data', value=combined_df.to_dict())
    context['task_instance'].xcom_push(key='coral_data_shape', value=combined_df.shape)
    context['task_instance'].xcom_push(key='failed_coral_projects', value=failed_projects)
    return "Coral data extraction successful"

def transform_data(**context):
    """Transform the extracted data"""
    logger = LoggingMixin().log
    logger.info("Starting data transformation")
    
    fish_data = context['task_instance'].xcom_pull(key='fish_data')
    df = pd.DataFrame.from_dict(fish_data)
    
    logger.info(f"Raw data columns: {df.columns.tolist()}")
    logger.info(f"Raw data shape: {df.shape}")
    
    transformed_df = transform_beltfish_data(df)
    
    logger.info("Data transformation completed")
    
    # Convert datetime columns to string format for XCom serialization
    for col in transformed_df.select_dtypes(include=['datetime64[ns]']).columns:
        transformed_df[col] = transformed_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    context['task_instance'].xcom_push(key='transformed_data', value=transformed_df.to_dict())
    return "Data transformation successful"

def transform_coral_task(**context):
    """Transform coral data task"""
    coral_data = context['task_instance'].xcom_pull(key='coral_data')
    if not coral_data:
        raise ValueError("No coral data found in XCom")
    
    df = pd.DataFrame.from_dict(coral_data)
    transformed_df = transform_coral_data(df)
    
    # Convert DataFrame to dict and ensure all values are JSON serializable
    transformed_dict = transformed_df.to_dict(orient='records')
    context['task_instance'].xcom_push(key='transformed_coral_data', value=transformed_dict)
    return "Coral data transformation successful"

def load_data(**context):
    """Load transformed data to database"""
    transformed_data = context['task_instance'].xcom_pull(key='transformed_data')
    df = pd.DataFrame.from_dict(transformed_data)
    
    db_config = DatabaseConfig()
    load_to_database(df, 'beltfish_surveys', db_config)
    return "Data loading successful"

def load_coral_data(**context):
    """Load transformed coral data to database"""
    transformed_data = context['task_instance'].xcom_pull(key='transformed_coral_data')
    df = pd.DataFrame.from_dict(transformed_data)
    
    db_config = DatabaseConfig()
    load_to_database(df, 'benthic_surveys', db_config)
    return "Coral data loading successful"

def extract_quadrat_data(**context):
    """Extract photo quadrat data from Mermaid API"""
    logger = LoggingMixin().log
    project_ids = context['dag_run'].conf.get('project_ids', get_rare_project_ids())
    
    logger.info(f"Fetching photo quadrat data for project IDs: {project_ids}")
    
    dfs = []
    failed_projects = []
    
    for project_id in project_ids:
        logger.info(f"Fetching photo quadrat data for project ID: {project_id}")
        try:
            df = fetch_mermaid_data(project_id, PHOTO_QUADRAT_ENDPOINT)
            if not df.empty:
                dfs.append(df)
                logger.info(f"Successfully fetched photo quadrat data for project ID: {project_id}")
        except Exception as e:
            logger.warning(f"Failed to fetch photo quadrat data for project ID {project_id}: {str(e)}")
            failed_projects.append(project_id)
            continue
    
    if not dfs:
        raise Exception("No photo quadrat data could be fetched from any project")
         
    combined_df = pd.concat(dfs, ignore_index=True)
    
    context['task_instance'].xcom_push(key='quadrat_data', value=combined_df.to_dict())
    context['task_instance'].xcom_push(key='quadrat_data_shape', value=combined_df.shape)
    context['task_instance'].xcom_push(key='failed_quadrat_projects', value=failed_projects)
    return "Photo quadrat data extraction successful"

def transform_quadrat_task(**context):
    """Transform photo quadrat data task"""
    quadrat_data = context['task_instance'].xcom_pull(key='quadrat_data')
    if not quadrat_data:
        raise ValueError("No photo quadrat data found in XCom")
    
    df = pd.DataFrame.from_dict(quadrat_data)
    transformed_df = transform_quadrat_data(df)  # Use the specific quadrat transform function
    
    transformed_dict = transformed_df.to_dict(orient='records')
    context['task_instance'].xcom_push(key='transformed_quadrat_data', value=transformed_dict)
    return "Photo quadrat data transformation successful"

def load_quadrat_data(**context):
    """Load transformed photo quadrat data to database"""
    transformed_data = context['task_instance'].xcom_pull(key='transformed_quadrat_data')
    df = pd.DataFrame.from_dict(transformed_data)
    
    db_config = DatabaseConfig()
    load_to_database(df, 'benthic_photo_quadrat_surveys', db_config)
    return "Photo quadrat data loading successful"

# Create the DAG
with DAG(
    'mermaid_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for DataMermaid API data',
    schedule_interval='0 2 * * *',  # Run once per day at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['mermaid', 'etl'],
) as dag:

    # Extract task
    extract_fish = PythonOperator(
        task_id='extract_fish_data',
        python_callable=extract_fish_data,
        provide_context=True,
    )

    # Transform task
    transform_fish = PythonOperator(
        task_id='transform_fish_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Load task
    load_fish = PythonOperator(
        task_id='load_fish_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Extract task
    extract_corals = PythonOperator(
        task_id='extract_coral_data',
        python_callable=extract_coral_data,
        provide_context=True,
    )

    # Transform task
    transform_corals = PythonOperator(
        task_id='transform_coral_data',
        python_callable=transform_coral_task,
        provide_context=True,
    )

    # Load task
    load_corals = PythonOperator(
        task_id='load_coral_data',
        python_callable=load_coral_data,
        provide_context=True,
    )

    extract_quadrats = PythonOperator(
        task_id='extract_quadrat_data',
        python_callable=extract_quadrat_data,
        provide_context=True
    )
    
    transform_quadrats = PythonOperator(
        task_id='transform_quadrat_data',
        python_callable=transform_quadrat_task,
        provide_context=True
    )
    
    load_quadrats = PythonOperator(
        task_id='load_quadrat_data',
        python_callable=load_quadrat_data,
        provide_context=True
    )

    # Set task dependencies
    extract_fish >> transform_fish >> load_fish
    extract_corals >> transform_corals >> load_corals
    extract_quadrats >> transform_quadrats >> load_quadrats 
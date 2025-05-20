import pandas as pd
import requests
from io import StringIO
from typing import Dict, Any
from sqlalchemy import create_engine, text, String
from config.mermaid_config import DatabaseConfig
import numpy as np

# API Endpoints
FISH_SURVEY_ENDPOINT = "beltfishes/obstransectbeltfishes"
CORAL_SURVEY_ENDPOINT = "benthicpits/obstransectbenthicpits"
PHOTO_QUADRAT_ENDPOINT = "benthicpqts/obstransectbenthicpqts"

# Fish text columns
FISH_TEXT_COLUMNS = [
    'project_name', 
    'project_admins', 
    'country_name', 
    'contact_link', 
    'tags',
    'site_name', 
    'reef_exposure', 
    'reef_slope', 
    'reef_type', 
    'reef_zone',
    'tide_name', 
    'visibility_name', 
    'current_name', 
    'relative_depth',
    'management_name', 
    'management_name_secondary', 
    'management_parties',
    'management_compliance', 
    'management_rules', 
    'label', 
    'transect_width_name',
    'observers', 
    'fish_family', 
    'fish_genus', 
    'fish_taxon', 
    'trophic_group',
    'functional_group', 
    'site_notes', 
    'management_notes', 
    'sample_unit_notes',
    'project_notes', 
    'data_policy_beltfish', 
    'site_id', 
    'id', 
    'project_id',
    'country_id', 
    'management_id', 
    'sample_event_id', 
    'sample_unit_id'
]

# Benthic text columns
BENTHIC_TEXT_COLUMNS = [
    'project_name',
    'project_admins',
    'country_name',
    'contact_link',
    'tags',
    'site_name',
    'reef_exposure',
    'reef_slope', 
    'reef_type',
    'reef_zone',
    'tide_name',
    'visibility_name',
    'current_name',
    'management_name',
    'management_name_secondary',
    'management_parties',
    'management_rules',
    'benthic_category',
    'benthic_attribute',
    'growth_form',
    'observers',
    'site_notes',
    'management_notes',
    'sample_unit_notes',
    'project_notes',
    'data_policy_benthicpit'
]

# Photo Quadrat text columns
PHOTO_QUADRAT_TEXT_COLUMNS = [
    'project_name',
    'project_admins',
    'country_name',
    'contact_link',
    'tags',
    'site_name',
    'reef_exposure',
    'reef_slope',
    'reef_type',
    'reef_zone',
    'tide_name',
    'visibility_name',
    'current_name',
    'relative_depth',
    'management_name',
    'management_name_secondary',
    'management_parties',
    'management_compliance',
    'management_rules',
    'label',
    'observers',
    'benthic_category',
    'benthic_attribute',
    'growth_form',
    'site_notes',
    'management_notes',
    'sample_unit_notes',
    'project_notes',
    'data_policy_benthicpqt',
    'site_id',
    'id',
    'project_id',
    'country_id',
    'management_id',
    'sample_event_id',
    'sample_unit_id'
]

def fetch_mermaid_data(project_id: str, endpoint: str) -> pd.DataFrame:
    """Fetch data from DataMermaid API"""
    url = f"https://api.datamermaid.org/v1/projects/{project_id}/{endpoint}/csv/"
    
    print(f"Fetching data from URL: {url}")
    try:
        response = requests.get(url)
        print(f"Response status code: {response.status_code}")
        response.raise_for_status()
        if response.text:
            print(f"First 500 characters of response: {response.text[:500]}")
            df = pd.read_csv(StringIO(response.text))
            if df.empty:
                print(f"Empty dataset received for project {project_id}")
            return df
        else:
            print("Empty response received")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {str(e)}")
        raise

def transform_beltfish_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform beltfish data according to requirements"""
    transformed_df = df.copy()
    
    # Combine date fields into a single datetime column
    transformed_df['date'] = pd.to_datetime(
        transformed_df['sample_date_year'].astype(str) + '-' + 
        transformed_df['sample_date_month'].astype(str) + '-' + 
        transformed_df['sample_date_day'].astype(str)
    )

    # Add time if available
    if 'sample_time' in transformed_df.columns:
        transformed_df['datetime'] = pd.to_datetime(
            transformed_df['date'].astype(str) + ' ' + 
            transformed_df['sample_time'].fillna('00:00:00')
        )

    # Handle numeric columns
    transformed_df['biomass_kgha'] = transformed_df['biomass_kgha'].fillna(0)

    # Log the transformation results
    print(f"Transformed data shape: {transformed_df.shape}")
    print(f"Transformed columns: {transformed_df.columns.tolist()}")
    print(f"First few rows of transformed data:\n{transformed_df.head()}")
    
    return transformed_df

def transform_coral_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform coral data according to requirements"""
    transformed_df = df.copy()
    
    # Combine date fields into a single datetime column
    transformed_df['date'] = pd.to_datetime(
        transformed_df['sample_date_year'].astype(str) + '-' + 
        transformed_df['sample_date_month'].astype(str) + '-' + 
        transformed_df['sample_date_day'].astype(str)
    )

    # Add time if available
    if 'sample_time' in transformed_df.columns:
        transformed_df['datetime'] = pd.to_datetime(
            transformed_df['date'].astype(str) + ' ' + 
            transformed_df['sample_time'].fillna('00:00:00')
        )

    # Convert datetime columns to string format for JSON serialization
    transformed_df['date'] = transformed_df['date'].dt.strftime('%Y-%m-%d')
    transformed_df['datetime'] = transformed_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Handle numeric columns specific to coral data
    if 'percent_cover' in transformed_df.columns:
        transformed_df['percent_cover'] = transformed_df['percent_cover'].fillna(0)

    # Log the transformation results
    print(f"Transformed coral data shape: {transformed_df.shape}")
    print(f"Transformed columns: {transformed_df.columns.tolist()}")
    print(f"First few rows of transformed data:\n{transformed_df.head()}")
    
    return transformed_df

def transform_quadrat_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform photo quadrat data according to requirements"""
    transformed_df = df.copy()
    
    # Combine date fields into a single datetime column, handling different formats
    try:
        # Ensure year is 4 digits
        transformed_df['sample_date_year'] = transformed_df['sample_date_year'].apply(
            lambda x: x if len(str(x)) == 4 else f"20{str(x).zfill(2)}"
        )

        transformed_df['date'] = pd.to_datetime(
            pd.DataFrame({
                'year': transformed_df['sample_date_year'],
                'month': transformed_df['sample_date_month'],
                'day': transformed_df['sample_date_day']
            })
        )

        # Add time if available
        if 'sample_time' in transformed_df.columns:
            transformed_df['datetime'] = pd.to_datetime(
                transformed_df['date'].astype(str) + ' ' + 
                transformed_df['sample_time'].fillna('00:00:00')
            )

        # Convert datetime columns to string format for JSON serialization
        transformed_df['date'] = transformed_df['date'].dt.strftime('%Y-%m-%d')
        transformed_df['datetime'] = transformed_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error processing dates: {str(e)}")
        print("Sample of problematic data:")
        print(transformed_df[['sample_date_year', 'sample_date_month', 'sample_date_day']].head())
        print("\nUnique values in date fields:")
        print("Years:", transformed_df['sample_date_year'].unique())
        print("Months:", transformed_df['sample_date_month'].unique())
        print("Days:", transformed_df['sample_date_day'].unique())
        raise

    # Handle numeric columns specific to photo quadrat data
    numeric_columns = ['quadrat_size', 'num_quadrats', 'num_points_per_quadrat', 'num_points']
    for col in numeric_columns:
        if col in transformed_df.columns:
            transformed_df[col] = transformed_df[col].fillna(0)

    # Log the transformation results
    print(f"Transformed photo quadrat data shape: {transformed_df.shape}")
    print(f"Transformed columns: {transformed_df.columns.tolist()}")
    print(f"First few rows of transformed data:\n{transformed_df.head()}")
    
    return transformed_df

def load_to_database(df: pd.DataFrame, table_name: str, db_config: DatabaseConfig) -> None:
    """Load DataFrame to PostgreSQL database"""
    connection_string = f"postgresql://{db_config.user}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}"
    engine = create_engine(connection_string)
    
    # Map pandas/numpy dtypes to PostgreSQL types
    dtype_mapping = {
        'object': 'TEXT',
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN'
    }
    
    # Use appropriate text columns based on table name
    if 'beltfish' in table_name.lower():
        text_columns = FISH_TEXT_COLUMNS
    elif 'benthicpqt' in table_name.lower():
        text_columns = PHOTO_QUADRAT_TEXT_COLUMNS
    else:
        text_columns = BENTHIC_TEXT_COLUMNS
    
    try:
        # Create schema and table first
        with engine.connect() as connection:
            with connection.begin():
                # Create schema if it doesn't exist
                connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS {db_config.schema}'))
                
                # Generate column definitions
                columns = []
                for column, dtype in df.dtypes.items():
                    pg_type = 'TEXT' if column in text_columns else dtype_mapping.get(str(dtype), 'TEXT')
                    columns.append(f'"{column}" {pg_type}')
                
                # Create table with proper schema
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {db_config.schema}.{table_name} (
                    {', '.join(columns)}
                )
                """
                connection.execute(text(create_table_sql))

        # Check if this project's data already exists
        project_ids = df['project_id'].unique()
        with engine.connect() as connection:
            for project_id in project_ids:
                result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {db_config.schema}.{table_name} WHERE project_id = :project_id"),
                    {"project_id": project_id}
                ).scalar()
                if result > 0:
                    print(f"Data for project {project_id} already exists. Skipping import.")
                    df = df[df['project_id'] != project_id]
        
        if df.empty:
            print("No new data to import")
            return

        # Insert data with correct column types
        df.to_sql(
            name=table_name,
            con=engine,
            schema=db_config.schema,
            if_exists='append',
            index=False,
            dtype={col: String for col in text_columns}
        )
    except Exception as e:
        raise Exception(f"Failed to load data to database: {str(e)}")
    finally:
        engine.dispose() 
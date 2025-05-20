# Mermaid ETL Pipeline

An Apache Airflow ETL pipeline for extracting, transforming, and loading MERMAID survey data into a PostgreSQL database. This repository is a minimal, production-ready deployment focused solely on the MERMAID pipeline.

## Project Structure

```
mermaid-etl/
├── dags/
│   └── mermaid_etl_dag.py
├── utils/
│   └── mermaid_utils.py
├── config/
│   ├── mermaid_config.py
│   └── credentials.template.py
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## Setup

1. **Clone the repository:**

```bash
git clone https://github.com/your-username/mermaid-etl.git
cd mermaid-etl
```

2. **Set up credentials:**
   - Copy `config/credentials.template.py` to `config/credentials.py`
   - Fill in your actual PostgreSQL and (optional) Mermaid API credentials in `credentials.py`
   - The credentials file is gitignored for security

```bash
cp config/credentials.template.py config/credentials.py
# Edit config/credentials.py with your actual credentials
```

3. **(Optional) Set environment variables**
   - If you need to override any Airflow or database settings, create a `.env` file or set environment variables as needed.

4. **Start Airflow and PostgreSQL via Docker Compose:**

```bash
docker-compose up -d
```

5. **Access Airflow UI:**
   - Open http://localhost:8080 in your browser
   - Default credentials: username `admin`, password `admin`

## Usage

### Scheduled Runs
The MERMAID pipeline (`mermaid_etl_pipeline`) is scheduled to run daily at 2 AM by default. Monitor progress and logs in the Airflow UI.

### Manual Triggers
You can manually trigger the pipeline via the Airflow UI or CLI:

```bash
airflow dags trigger mermaid_etl_pipeline
```

## Features

- **Automated Project Discovery:** Fetches MERMAID project IDs from the DataMermaid API using the "Rare" tag
- **Parallel Data Processing:** Handles multiple projects in parallel for fish, coral, and photo quadrat surveys
- **Data Integrity:** Prevents duplicate imports and ensures robust type conversions (datetime, UUID, numeric)
- **Performance:** Chunked processing and bulk inserts for large datasets
- **UPSERT Support:** Maintains data consistency in the target PostgreSQL database
- **Error Handling & Logging:** Comprehensive logging and error management throughout the pipeline
- **Configurable:** All credentials and most settings are managed through config files and environment variables

## Dependencies

- Apache Airflow
- PostgreSQL
- Python 3.8+
- Pandas
- SQLAlchemy
- Requests
- psycopg2
- numpy

## Pipeline Details

### MERMAID Pipeline
Extracts data from the DataMermaid API for projects tagged with 'Rare'. Processes three types of surveys: fish, coral, and photo quadrat.

### OurFish Pipeline
Transfers catch data from the OurFish production PostgreSQL database on AWS RDS to a DigitalOcean managed PostgreSQL database. Data passes through CSV files and XCom between tasks.

### KoboToolbox Pipeline
Extracts data from KoboToolbox forms, transforms it according to project-specific requirements, and loads it into the PostgreSQL database. Currently configured for fisher registration data, but extensible to other Kobo projects.

## Credentials Management

### Centralized Credentials System
All sensitive information is stored in a single file (`config/credentials.py`) that is excluded from version control:

- **Database Credentials**: All database connection details are stored in the `DB_CREDENTIALS` dictionary
- **API Tokens**: API keys and tokens are stored in dedicated variables or dictionaries
- **Fallback Mechanism**: All config files include fallback logic if the credentials file is missing

### Adding New Credentials

1. Update the `credentials.py` file with your new credentials
2. If adding a new type of credential, also update the `credentials.template.py` file
3. Reference the credentials in your config files using the import pattern:

```python
try:
    from config.credentials import DB_CREDENTIALS
except ImportError:
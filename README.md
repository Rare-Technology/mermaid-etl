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

## Data Sources

This pipeline currently extracts data from the following public endpoints:

- `FISH_SURVEY_ENDPOINT = "beltfishes/obstransectbeltfishes"`
- `CORAL_SURVEY_ENDPOINT = "benthicpits/obstransectbenthicpits"`
- `PHOTO_QUADRAT_ENDPOINT = "benthicpqts/obstransectbenthicpqts"`

More endpoints will be added soon.

## Database Schema

This pipeline automatically creates the required table structures in your PostgreSQL database. By default, it assumes there is a schema called `mermaid_source`. You can change the schema name by editing the `schema` attribute in `config/mermaid_config.py`.

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

- **Automated Project Discovery:** Fetches MERMAID project IDs from the DataMermaid API using the "Org" tag
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
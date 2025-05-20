from dataclasses import dataclass

# Import centralized credentials
try:
    from config.credentials import DB_CREDENTIALS, API_CREDENTIALS
except ImportError:
    # Fallback for when credentials file doesn't exist
    print("WARNING: credentials.py not found. Using empty credentials.")
    DB_CREDENTIALS = {
        "postgres": {
            "host": "", "port": 5432, "database": "", "user": "", "password": ""
        }
    }
    API_CREDENTIALS = {"mermaid": {"base_url": "https://api.datamermaid.org/v1"}}

@dataclass
class DatabaseConfig:
    # Default values from centralized credentials
    host: str = DB_CREDENTIALS.get("postgres", {}).get("host", "")
    port: int = DB_CREDENTIALS.get("postgres", {}).get("port", 5432)
    database: str = DB_CREDENTIALS.get("postgres", {}).get("database", "")
    user: str = DB_CREDENTIALS.get("postgres", {}).get("user", "")
    password: str = DB_CREDENTIALS.get("postgres", {}).get("password", "")
    schema: str = "mermaid_source" # <-- Change 'mermaid_source' to your schema name

# API Configuration
MERMAID_API_BASE_URL = API_CREDENTIALS.get("mermaid", {}).get("base_url", "https://api.datamermaid.org/v1/projects")
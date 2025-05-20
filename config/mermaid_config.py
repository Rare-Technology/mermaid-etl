from dataclasses import dataclass

# Import centralized credentials
try:
    from config.credentials import DB_CREDENTIALS, API_CREDENTIALS
except ImportError:
    # Fallback for when credentials file doesn't exist
    print("WARNING: credentials.py not found. Using empty credentials.")
    DB_CREDENTIALS = {
        "digitalocean": {
            "host": "", "port": 5432, "database": "", "user": "", "password": ""
        }
    }
    API_CREDENTIALS = {"mermaid": {"base_url": "https://api.datamermaid.org/v1"}}

@dataclass
class DatabaseConfig:
    # Default values from centralized credentials
    host: str = DB_CREDENTIALS.get("digitalocean", {}).get("host", "")
    port: int = DB_CREDENTIALS.get("digitalocean", {}).get("port", 5432)
    database: str = DB_CREDENTIALS.get("digitalocean", {}).get("database", "")
    user: str = DB_CREDENTIALS.get("digitalocean", {}).get("user", "")
    password: str = DB_CREDENTIALS.get("digitalocean", {}).get("password", "")
    schema: str = "mermaid_source"

@dataclass
class EmailConfig:
    # These should be moved to credentials.py in production
    smtp_host: str = "smtp.smtp2go.com"
    smtp_port: int = 587
    smtp_user: str = "data@rare.org\\gstoyle@rare.org"
    smtp_password: str = ""  # Should be filled from credentials
    recipient_email: str = "gstoyle@rare.org"

# API Configuration
MERMAID_API_BASE_URL = API_CREDENTIALS.get("mermaid", {}).get("base_url", "https://api.datamermaid.org/v1/projects")
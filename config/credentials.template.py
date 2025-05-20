"""
TEMPLATE FILE - DO NOT MODIFY
Copy this file to credentials.py and fill in your actual credentials.
The credentials.py file should be added to .gitignore to prevent credentials from being committed.
"""

# Database credentials
DB_CREDENTIALS = {
    # PostgreSQL for destination database
    "postgres": {
        "host": "your-postgres-host",
        "port": 1234,
        "database": "your_database",
        "user": "your_username",
        "password": "your_password"
    },
    
    # Add more database configurations as needed
}

# API credentials
API_CREDENTIALS = {
    "mermaid": {
        "api_key": "",  # If needed
        "base_url": "https://api.datamermaid.org/v1"
    },
    # Add more API configurations as needed
}

import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

# Import static constants from separate file
from constants import COMPANY_ALIASES, QC_TAGS, BLOG_PLATFORMS, EVENT_KEYWORDS

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

# -----------------------------
# Database Configuration
# -----------------------------
ps_host = os.environ["PS_HOST"]
ps_port = int(os.environ["PS_PORT"])
ps_user = os.environ["PS_USER"]
ps_password = os.environ["PS_PASSWORD"]
ps_dbname = os.environ["PS_DBNAME"]

# -----------------------------
# API Keys
# -----------------------------
NEWS_API_KEY = os.environ["NEWS_API_KEY"]

# -----------------------------
# AWS Configuration
# -----------------------------
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
BEDROCK_REGION = os.environ["BEDROCK_REGION"]

# -----------------------------
# AWS Bedrock Configuration
# -----------------------------
TITAN_EMBED_MODEL_ID = 'amazon.titan-embed-text-v2:0'
CLAUDE_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# -----------------------------
# Deduplication Settings
# -----------------------------
DUP_THRESHOLD = 0.60

# -----------------------------
# Output Configuration
# -----------------------------

PROJECT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = PROJECT_DIR / "Outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

BASE_FOLDER = OUTPUTS_DIR / f"{datetime.now().strftime('%Y%m%d')}_NewsAPI_extraction"
BASE_FOLDER.mkdir(parents=True, exist_ok=True)

RUN_STATS_FILE = PROJECT_DIR / "pipeline_run_statistics.xlsx"


# -----------------------------
# Run Tracking Configuration
# -----------------------------
RUN_STATS_FILE = PROJECT_DIR / "pipeline_run_stats.xlsx"


# -----------------------------
# Helper Functions
# -----------------------------
def run_query(sql_query):
    """Execute SQL query and return results as DataFrame"""
    try:
        engine = create_engine(f"postgresql://{ps_user}:{ps_password}@{ps_host}:{ps_port}/{ps_dbname}")
        df = pd.read_sql_query(text(sql_query), engine)
        return df
    except Exception as e:
        print("Error:", e)
        return None


def get_table_config(mode):
    """
    Get database table and field configuration based on mode.
    
    Args:
        mode (str): Either 'supplier' or 'location'
        
    Returns:
        tuple: (table_name, field_string) for SQL queries
    """
    if mode == "supplier":
        return ("companies", "company_name, full_name")
    elif mode == "location":
        return ("locations", "location AS company_name, location AS full_name")
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'supplier' or 'location'")


def get_id_field_config(mode):
    """
    Get ID field configuration for final data enrichment.
    
    Args:
        mode (str): Either 'supplier' or 'location'
        
    Returns:
        tuple: (table_name, field_string) for SQL queries
    """
    if mode == "supplier":
        return ("companies", "company_name, company_id")
    elif mode == "location":
        return ("locations", "location AS company_name, location_id as company_id")
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'supplier' or 'location'")


# -----------------------------
# Data Source Configuration
# -----------------------------
# Configure which data sources to fetch
# Set to empty list [] to skip that source

# Companies to fetch (set to [] to skip)
COMPANIES_LIST = []  # Will be populated below
LOCATIONS_LIST = []  # Will be populated below

# Determine which sources to fetch
FETCH_COMPANIES = True  # Set to False to skip companies
FETCH_LOCATIONS = True  # Set to True to also fetch locations

# Populate lists based on configuration
if FETCH_COMPANIES:
    table, field = get_table_config("supplier")
    companies_df = run_query(f"SELECT {field} FROM {table} ORDER BY company_name ASC;")
    if companies_df is not None:
        COMPANIES_LIST = list(companies_df["full_name"].unique())[:20]
        print(f'COMPANIES to fetch: {len(COMPANIES_LIST)}')

if FETCH_LOCATIONS:
    table, field = get_table_config("location")
    locations_df = run_query(f"SELECT {field} FROM {table} ORDER BY company_name ASC;")
    if locations_df is not None:
        LOCATIONS_LIST = list(locations_df["full_name"].unique())[:20]
        print(f'LOCATIONS to fetch: {len(LOCATIONS_LIST)}')

# -----------------------------
# News API Configuration
# -----------------------------
days_back = 1
cutoff_time = (pd.Timestamp.now(tz="Asia/Kolkata") - pd.Timedelta(days=days_back)).normalize() + pd.Timedelta(hours=13)
print("cutoff_time (IST):", cutoff_time)

MAX_ITEMS = 100  # Maximum articles to fetch per company/location
LANGUAGE = "eng"  # Language code for news articles
DATA_TYPES = ["news", "pr"]  # Types of content to fetch
CATEGORY = "Business & Finance"  # News category filter

# -----------------------------
# Retry Configuration
# -----------------------------
MAX_RETRIES = 5  # Maximum retry attempts for API calls
RETRY_DELAY = 3  # Delay in seconds between retries

# -----------------------------
# URL Checking Configuration
# -----------------------------
URL_CHECK_MAX_WORKERS = 20  # Number of concurrent threads for URL checking
URL_CHECK_TIMEOUT = 5  # Timeout in seconds for URL checks

print(f"EVENT_KEYWORDS count: {len(EVENT_KEYWORDS)}")

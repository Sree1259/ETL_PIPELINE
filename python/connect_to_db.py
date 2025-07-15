import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils import FileUtils
# Load environment variables
load_dotenv()
# Initialize logger
logger = FileUtils.set_logger("connect_to_pgsql")
def construct_db_uri():
    """Constructs the PostgreSQL database URI."""
    try:
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    except Exception as ex:
        logger.error(f"Error constructing DB URI: {ex}")
        raise
def get_db_connection():
    """Returns a SQLAlchemy database engine for PostgreSQL connection."""
    try:
        logger.info("establishing db_connection")
        DB_URI = construct_db_uri()
        return create_engine(DB_URI)
    except Exception as ex:
        logger.error(f"Error creating DB connection: {ex}")
        raise

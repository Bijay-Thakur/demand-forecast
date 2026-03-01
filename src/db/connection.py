import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine()->Engine:
    """
    Create a SQLAlchemy engine for Postgres using .env values.
    """
    load_dotenv()
    
    host=os.getenv("DB_HOST","localhost")
    port=os.getenv("DB_PORT","5432")
    name=os.getenv("DB_NAME","demand_forecasting")
    user=os.getenv("DB_USER","postgres")
    password=os.getenv("DB_PASSWORD")
    
    if not password:
        raise ValueError("DB_PASSWORD is not set in the environment variables.")
    url=f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url,pool_pre_ping=True)    
    
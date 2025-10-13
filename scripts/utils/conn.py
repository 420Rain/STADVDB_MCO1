import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection(config=None):
  config = config or {}

  conn_params = {
    "host": config.get("host", os.getenv("DB_HOST", "localhost")),
    "port": config.get("port", os.getenv("DB_PORT", "5432")),
    "user": config.get("user", os.getenv("DB_USER")),
    "password": config.get("password", os.getenv("DB_PASSWORD")),
    "dbname": config.get("dbname", os.getenv("DB_DATABASE")),
  }

  conn = psycopg2.connect(**conn_params)
  return conn

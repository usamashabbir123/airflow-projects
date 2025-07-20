import os
import psycopg2
import psycopg2.extras
from .db import DatabaseConnector



class PostgresConnection(DatabaseConnector):
    def __init__(self):
        # Read creds from env vars
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "")
        self.database = os.getenv("POSTGRES_DB", "postgres")
        self.conn = None
        print(f"PostgreSQL connection params: {self.host}:{self.port}")

    def connect(self):
        print('Connecting to PostgreSQL database...')
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                cursor_factory=psycopg2.extras.RealDictCursor  # Returns dict-like rows
            )
            self.cursor = self.conn.cursor()
            print('PostgreSQL connection established.')
            return self.conn
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise

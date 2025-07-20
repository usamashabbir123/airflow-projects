import os
import pymysql
from .db import DatabaseConnector
class MySqlConnector(DatabaseConnector):
    def __init__(self):
        # Read creds from env vars
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", 3306))
        self.user = os.getenv("DB_USER", "root")
        self.password = os.getenv("DB_PASSWORD", "")
        self.database = os.getenv("DB_NAME", "test")
        self.conn = None
        print(self.host, self.port, self.user, self.password, self.database)

    def connect(self):
        print('Connecting to MySQL database...')
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.conn.cursor()
        print('Connection established.')
        return self.conn
    

from abc import ABC, abstractmethod
class BaseConnection(ABC):
    def __init__(self):
        self.conn= None
        self.cursor = None
    @abstractmethod
    def connect(self):
        """Establish a connection to the database."""
        pass
    def close(self):
        """Close the connection to the database."""
        if self.conn:
            self.conn.close()
        if self.cursor:
            self.cursor.close()
    def execute_query(self, query: str):
        """Execute a query on the database."""
        return self.cursor.execute(query)
    def execute_query_with_params(self, query: str, params: tuple):
        """Execute a query with parameters on the database."""
        return self.cursor.execute(query, params)

    def fetch_all(self):
        """Fetch all results from the last executed query."""
        return self.cursor.fetchall()
    def fetch_one(self):
        """Fetch one result from the last executed query."""
        return self.cursor.fetchone()
    def fetch_many(self, size: int):
        """Fetch a specified number of results from the last executed query."""
        return self.cursor.fetchmany(size)
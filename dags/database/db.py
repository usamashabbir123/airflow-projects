from abc import ABC, abstractmethod
class DatabaseConnector(ABC):
    """
    Generic DB connector for Airflow.
    Supports Postgres, MySQL, MSSQL (any hook implementing get_conn).
    """
    def __init__(self):
        """
        :param conn_id: Airflow connection id
        """
        self.conn = None
        self.cursor = None
    @abstractmethod
    def connect(self):
        """
        Establish a database connection.
        """
        pass
    def execute(self, sql, params=None):
        if not self.conn:
            raise RuntimeError("Connection not established. Call connect() first.")
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()
    def fetchmany(self, size):
        return self.cursor.fetchmany(size)

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
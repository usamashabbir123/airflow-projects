from connections.base.db_base import BaseConnection
import pandas as pd 
    
class PostgresConnector(BaseConnection):
    batch_size = 500
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
    def connect(self):
        import psycopg2
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.database
            )
            print("Connection to PostgreSQL established successfully.")
            self.conn= connection
            self.cursor = connection.cursor()
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
    def load_data(self):
        """Load data from CSV and insert into PostgreSQL database."""
        try:
            # Verify connection first
            if self.conn is None or self.cursor is None:
                raise Exception("Database connection not established. Call connect() first.")

            # Check if file exists
            import os
            file_path = '/opt/airflow/dags/datasets/airnub/05_2020.csv'
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at {file_path}")

            # Iterator setup with error handling
            csv_iterator = pd.read_csv(
                file_path,
                chunksize=self.batch_size,
                on_bad_lines='warn'
            )
    
            rows_processed = 0
            for chunk_number, chunk in enumerate(csv_iterator, 1):
                try:
                    start_row = rows_processed
                    rows_processed += len(chunk)
                    print(f"Processing batch {chunk_number}: rows {start_row} to {rows_processed}")
                    self.insert_data(chunk)
                except Exception as e:
                    print(f"Error processing batch {chunk_number}: {e}")
                    self.conn.rollback()
                    raise

            print(f"Successfully processed {rows_processed} total rows")

        except Exception as e:
            print(f"Error in load_data: {e}")
            raise
    def insert_data(self, df):
        """Insert data into the PostgreSQL database."""
        if self.conn is None or self.cursor is None:
            raise Exception("Connection not established. Call connect() first.")
        try:
            insert_query = f"""
         INSERT INTO bronze.airbnb_listings_raw (
    LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME, HOST_SINCE, 
    HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD, LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, 
    ROOM_TYPE, ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30, 
    NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, 
    REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN, 
    REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE
) 
VALUES (
    %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, 
    %s, %s, %s, 
    %s, %s, 
    %s, %s
);

            """
            
            # Insert rows
            for index, row in df.iterrows():
                print(row)
                self.cursor.execute(insert_query, tuple(row))
            self.conn.commit()
            print(f"Inserted batch with {len(df)} rows successfully.")
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()
    def load_data_from_Census_LGA(self,file_Path,table_name):
        """Load data from Census LGA CSV and insert into PostgreSQL database."""
        try:
            if self.conn is None or self.cursor is None:
                raise Exception("Database connection not established. Call connect() first.")
            
            # Read the CSV file
            G01_iter = pd.read_csv(file_Path,chunksize=self.batch_size, on_bad_lines='warn')

            for chunk_number, df in enumerate(G01_iter, 1):
                try:
                    print(f"Processing batch {chunk_number}: {len(df)} rows")
                    self.insert_data_into_Census_LGAG(df,table_name)
                except Exception as e:
                    print(f"Error processing batch {chunk_number}: {e}")
                    self.conn.rollback()
                    raise
        except Exception as e:
            print(f"Error in load_data_from_Census_LGA: {e}")
            raise

    def insert_data_into_Census_LGAG(self, df,table_name):
        """Insert data into the Census LGA tables."""
        if self.conn is None or self.cursor is None:
            raise Exception("Connection not established. Call connect() first.")
        try:
            df = df.dropna(axis=1, how='all') 
            columns = df.columns 
            print(f"columns: {columns}")
            querry_columns = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"""
            INSERT INTO bronze.{table_name} ({querry_columns})
            VALUES ({placeholders});
            """
            for _, row in df.iterrows():
                self.execute_query_with_params(insert_query, tuple(row))
            self.conn.commit()
            print(f"Inserted {len(df)} rows into Census LGA {table_name} table successfully.")
        except Exception as e:
            print(f"Error inserting data into Census LGA: {e}")
            self.conn.rollback()
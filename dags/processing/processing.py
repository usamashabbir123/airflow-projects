import pandas as pd 
from database.postgres import PostgresConnection
class DataClearing():
    batch_size = 200  # tune this to your needs
    def __init__(self,conn):
        self.data= []
        self.conn = conn

    def load_data(self):
        self.conn.execute("SELECT * FROM raw_data")
        print('Data loaded from raw_data table of batch size',self.batch_size)
        count=0
        while True:
            rows = self.conn.fetchmany(self.batch_size)
            if not rows:
                break
            count += 1  
            print(f"Batch No {count} loaded with {len(rows)} records")
            for row in rows:
                print(row)
                self.data=rows
                self.transform_data()
                self.save_data()
    def transform_data(self):
        try:
            # Convert data to DataFrame
            df = pd.DataFrame(self.data)
            print("Initial data shape:", df.shape)
            print("Initial data :", df)
            
            # Handle age column
            # Calculate median age for valid ages (between 0 and 100)
            age_median = df.loc[(df['age'] > 0) & (df['age'] <= 100), 'age'].median()
            
            # Replace invalid ages with median
            df.loc[(df['age'] <= 0) | (df['age'] > 100), 'age'] = age_median
            
            # Handle missing values
            df['age'] = df['age'].fillna(age_median)
            
            # Optional: handle salary if needed
            # if 'salary' in df.columns:
            #     salary_mean = df['salary'].mean()
            #     df['salary'] = df['salary'].fillna(salary_mean)

            df['created_at'] = df['created_at'].fillna(pd.Timestamp.now())  
            self.data= df.to_dict(orient='records')
            print('data cleaning completed',self.data)            
        except Exception as e:
            print(f"Error in data transformation: {str(e)}")
            raise
    def save_data(self):
        pc = None
        try:
            pc = PostgresConnection()
            pc.connect()
            print('✅ PostgreSQL connected')

            # Create table if it doesn't exist
            create_table_query = '''
                CREATE TABLE IF NOT EXISTS cleaned_data (
                    id INTEGER PRIMARY KEY,
                    age INTEGER,
                    name VARCHAR(255),
                    email VARCHAR(255),
                    created_at TIMESTAMP
                );
            '''
            pc.execute(create_table_query)
            pc.commit()
            print('✅ Table created or already exists.')

            # Prepare insert query with ON CONFLICT
            insert_query = '''
                INSERT INTO cleaned_data (id, name, email, age, created_at)
                VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(created_at)s)
                ON CONFLICT (id) DO NOTHING;
            '''

            # Insert records one by one
            for record in self.data:
                pc.execute(insert_query, record)

            pc.commit()
            print(f'✅ Data saved successfully: {len(self.data)} records.')

        except Exception as e:
            if pc:
                pc.rollback()  # Rollback on error
            print(f"❌ Error saving data: {str(e)}")
            raise

        finally:
            if pc:
                pc.close()
                print("🔒 Connection closed.")


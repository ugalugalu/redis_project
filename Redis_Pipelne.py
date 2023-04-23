import pandas as pd
import psycopg2
import redis
from sqlalchemy import create_engine

redis_client = redis.Redis(host='localhost', port=6379)

# Postgres Database Information
connection_string = 'postgresql://postgres:ubo123@34.173.130.172:5432/postgres'

def extract_data():
        """
       Returns a dataframe after extracting a CSV file
            
            Parameters:
                    doesnot take in any parameters
            Returns:
                    Returns a Data Frame
            """
    # Extract data from CSV file using pandas
        df = pd.read_csv('customer_call_logs.csv')
        print('Extraction Successful')
        return df

def transform_data():

    """
        Returns a cleaned Dataframe. Stores a dataframe temporarily in Redis Cache Before transformation
            
            Parameters:
                    Dataframe
            Returns:
                    Returns a Dataframe
    """
        # Check Redis cache first
    cached_data = redis_client.get('transformed_data')
    if cached_data is not None:
            df = pd.read_json(cached_data, orient='records')
            return df

    # If not cached, perform transformations on extracted data
    df = extract_data()

    # Apply transformations by stripping dollar sign, converting string to float and converting dollars to KSH(Rate used is 1dollar = 134 ksh)
    df['call_cost'] = df['call_cost'].str.lstrip('$').astype(float)*134

    # Cache transformed data in Redis
    redis_client.set('transformed_data', df.to_json(orient='records'))
    print('Transformation Successful')
    return df

def load_data(df):
    """
    Loads a dataframe to a postgre Database Hosted in Google Cloud.
            
            Parameters:
                    Dataframe(the transformed Data Frame),connection string to the database
            Returns:
                    None
    """
   
    # Connect to the database
    engine = create_engine('postgresql://postgres:ubo123@34.173.130.172:5432/postgres')
   
    
    # Write the dataframe to the database
    df.to_sql('call_records', engine, if_exists='replace', index=False)
    print('Loading Successful')

def read_data():
      """
    Returns a cleaned Dataframe.
            
            Parameters:
                    connection string to the database
            Returns:
                    Returns a Dataframe
       """
     #Create a connection to the database using pyscop
      conn = psycopg2.connect(connection_string)
      #Create the query to be executed
      query = "SELECT * FROM call_records"
      # Execute query,create a dataframe and display the five records.
      queried_frame = pd.read_sql(query,conn)
      print(queried_frame.head())

def data_pipeline():
    # Data pipeline function
    extract_data()
    transformed_data = transform_data()
    load_data(transformed_data)
    read_data()

if __name__ == '__main__':
    # Run the data pipeline function
    data_pipeline()
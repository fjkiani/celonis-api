import logging
import pandas as pd
import re
import subprocess
import shutil
import os
from datetime import datetime
from pycelonis import get_celonis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define constants from environment variables
BASE_URL = os.getenv('BASE_URL')
API_TOKEN = os.getenv('API_TOKEN')

# Define other constants
DATA_POOL_NAME = 'test1'
TABLE_NAME = 'TestTable'
INPUT_FILE_PATH = '/Users/fahadkiani/Desktop/development/celonis/input/raw.txt'
OUTPUT_FILE_PATH = '/Users/fahadkiani/Desktop/development/celonis/cleaned_data.csv'
CLEANED_FILE_PATH = '/Users/fahadkiani/Desktop/development/celonis/processed/cleaned_processed_claims_data_comprehensive.csv'
UNPROCESSED_DIR = '/Users/fahadkiani/Desktop/development/celonis/unprocessed/'
PROCESSED_DIR = '/Users/fahadkiani/Desktop/development/celonis/processed/'
LOG_FILE_PATH = '/Users/fahadkiani/Desktop/development/celonis/logs/process.log'

# Create directories if they don't exist
os.makedirs(UNPROCESSED_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE_PATH),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def clean_data(input_file, output_file):
    logger.info(f'Starting to clean data from {input_file}')
    data = []

    # Read the file line by line
    with open(input_file, 'r', encoding='utf-8') as file:
        for line in file:
            # Skip the lines that are not data rows
            if re.match(r'^\|\s*\|\d{3}\s*\|', line):
                # Remove leading and trailing whitespace and split the row by '|'
                row = [field.strip() for field in line.strip().split('|')]
                # Append the cleaned row to the data list
                data.append(row[2:-1])  # Skip the first two and last column which are empty

    # Define the column names based on the file structure
    columns = ['MANDT', 'QMNUM', 'QMART', 'QMTXT', 'ARTPR', 'PRIOK', 'ERNAM', 'ERDAT', 
               'AENAM', 'AEDAT', 'MZEIT', 'QMDAT', 'QMNAM', 'STRMN', 'STRUR', 
               'LTRMN', 'LTRUR', 'WAERS', 'AUFNR', 'VERID']

    # Create a DataFrame from the data
    df = pd.DataFrame(data, columns=columns)

    # Remove rows with null values in the primary key column
    df_cleaned = df.dropna(subset=['QMNUM'])

    # Ensure QMNUM is string type
    df_cleaned['QMNUM'] = df_cleaned['QMNUM'].astype(str)

    # Save the cleaned DataFrame to a CSV file
    df_cleaned.to_csv(output_file, index=False)

    logger.info(f'Data has been cleaned and saved to {output_file}')
    return df_cleaned

def move_file(src, dest):
    try:
        shutil.move(src, dest)
        logger.info(f'Successfully moved {src} to {dest}')
    except Exception as e:
        logger.error(f'Error moving file {src} to {dest}: {e}')

def get_or_create_data_pool(celonis, data_pool_name):
    data_pool = celonis.data_integration.get_data_pools().find(data_pool_name)
    if data_pool:
        logger.info(f"Found existing data pool: {data_pool_name}")
        return data_pool
    else:
        logger.info(f"Creating new data pool: {data_pool_name}")
        return celonis.data_integration.create_data_pool(name=data_pool_name)

def push_data_to_celonis(celonis, data_pool_name, table_name, df):
    try:
        pool = get_or_create_data_pool(celonis, data_pool_name)

        if not pool:
            logger.error(f"Unable to get or create data pool '{data_pool_name}'.")
            return

        # Generate a unique table name using timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_table_name = f"{table_name}_{timestamp}"

        logger.info(f"Creating new table '{unique_table_name}' in data pool '{data_pool_name}'.")

        # Create the new table in the data pool
        data_pool_table = pool.create_table(df, unique_table_name)

        # Append the DataFrame to the new table
        data_pool_table.append(df)

        logger.info(f"Data successfully pushed to new table {data_pool_name}.{unique_table_name}")

    except Exception as e:
        logger.error(f"Error pushing data: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

def main():
    logger.info('Starting main process')
    
    # Step 1: Clean the initial raw data
    df_cleaned = clean_data(INPUT_FILE_PATH, OUTPUT_FILE_PATH)
    
    # Move the raw data file to the unprocessed directory
    move_file(INPUT_FILE_PATH, os.path.join(UNPROCESSED_DIR, 'raw.txt'))

    # Step 2: Invoke the external clean_data2.py script to further process the cleaned data
    logger.info('Invoking clean_data2.py script')
    subprocess.run(['python', 'clean_data2.py'])

    # Move the cleaned data file to the processed directory
    move_file(OUTPUT_FILE_PATH, os.path.join(PROCESSED_DIR, 'cleaned_data.csv'))
    move_file(CLEANED_FILE_PATH, os.path.join(PROCESSED_DIR, 'cleaned_processed_claims_data_comprehensive.csv'))

    # Load the final cleaned data
    df_final_cleaned = pd.read_csv(os.path.join(PROCESSED_DIR, 'cleaned_processed_claims_data_comprehensive.csv'))
    
    # Step 3: Initialize Celonis connection
    logger.info('Initializing Celonis connection')
    celonis = get_celonis(
        base_url=BASE_URL,
        api_token=API_TOKEN
    )
    
    # Step 4: Push data to Celonis
    logger.info('Pushing data to Celonis')
    push_data_to_celonis(celonis, DATA_POOL_NAME, TABLE_NAME, df_final_cleaned)

if __name__ == '__main__':
    main()

# Claims Data Import to Celonis

This project provides a solution for importing claims data from a flat file into the Celonis platform. The solution includes data cleaning, file management, logging, and automated data upload to Celonis.

## Overview

The solution performs the following steps:

1. **File Management**: Moves raw data files to an `unprocessed` folder after initial cleaning and moves cleaned data files to a `processed` folder after further processing.
2. **File Processing**: Cleans the data by handling encoding, missing values, and other necessary transformations.
3. **Data Upload to Celonis**: Uses the Celonis Data Push API to upload the cleaned data to the Celonis platform.
4. **Logging**: Provides comprehensive logging to help troubleshoot any issues that may arise during the process.

## Directory Structure

/path/to/project
├── clean_data2.py
├── main.py
├── input/
│ └── raw.txt
├── unprocessed/
├── processed/
├── README.md



## Prerequisites

- Python 3.x
- Required Python packages: pandas, re, requests, pycelonis, logging, shutil, os

## Setup

1. **Clone the repository**:

   ```sh
   git clone https://github.com/your-repo/claims-data-import.git
   cd claims-data-import

## Install dependencies:

pip install pandas requests pycelonis

## Update configuration:

Open main.py and .env and update the following constants with your specific values:

BASE_URL
API_TOKEN
DATA_POOL_NAME
TABLE_NAME
INPUT_FILE_PATH
OUTPUT_FILE_PATH
CLEANED_FILE_PATH
UNPROCESSED_DIR
PROCESSED_DIR


## Usage
Place your raw data file in the input/ directory.
Ensure the file is named raw.txt or update the INPUT_FILE_PATH constant in main.py.

python main.py

This will perform the following steps:

-Clean the initial raw data.
-Move the raw data file to the unprocessed/ directory.
-Invoke the clean_data2.py script to further process the cleaned data.
-Move the cleaned data files to the processed/ directory.
-Upload the cleaned data to Celonis using the Data Push API.


## Logging
The script provides comprehensive logging to track each step and make it easier to troubleshoot if something goes wrong. Logs are printed to the console for real-time monitoring.

Functions
clean_data(input_file, output_file)
-Reads and cleans the raw data from the specified input file.
-Saves the cleaned data to the specified output file.
-Returns a DataFrame of the cleaned data.

move_file(src, dest)
-Moves a file from the source path to the destination path.
-Logs the success or failure of the file move operation.

-get_or_create_data_pool(celonis, data_pool_name)
-Retrieves an existing data pool or creates a new one in Celonis.
-Returns the data pool object.

-push_data_to_celonis(celonis, data_pool_name, table_name, df)
-Pushes the cleaned data to Celonis.
-Creates a new table with a unique name using a timestamp.
-Logs the success or failure of the data push operation.


## automation 

To automate the execution of this script, you can use various methods depending on your operating system and the environment where the script will run. Here are some common methods:

Using Cron Jobs (Linux/Unix)
You can schedule the script to run at regular intervals using cron.

Open the cron table:
crontab -e


Add a new cron job:
For example, to run the script every day at 2 AM, add the following line:
0 2 * * * /usr/bin/python3 /path/to/your/project/main.py

## Create a scheduling script:

import schedule
import time
import subprocess

def job():
    subprocess.call(["python", "/path/to/project/main.py"])

# Schedule the job every day at 2 AM
schedule.every().day.at("02:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

Run the scheduling script:

python scheduler.py

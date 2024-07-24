# clean_data.py

import pandas as pd
import logging

logger = logging.getLogger('DataCleaner')

def clean_data(input_file, output_file):
    logger.info(f"Starting to clean data from file: {input_file}")
    
    cleaned_lines = []
    total_lines = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            for i, line in enumerate(file):
                total_lines += 1
                line = line.strip()
                
                # Skip empty lines and separator lines
                if not line or line.startswith('---'):
                    continue
                
                # Remove leading and trailing '|' and split by '|'
                fields = line.strip('|').split('|')
                
                # Remove the empty field at the beginning
                if fields and fields[0] == '':
                    fields = fields[1:]
                
                if len(fields) == 20:  # Assuming 20 columns
                    cleaned_lines.append(fields)
                else:
                    logger.warning(f"Line {i} has incorrect number of fields: {len(fields)} - Content: {line}")
                
                if i < 5:  # Log first 5 valid lines for debugging
                    logger.debug(f"Sample line {i}: {fields}")

        logger.info(f"Total lines read from file: {total_lines}")
        logger.info(f"Total cleaned lines: {len(cleaned_lines)}")

        if not cleaned_lines:
            logger.warning("No valid data found in the input file.")
            return None

        # Define column names based on your data structure
        columns = ['MANDT', 'QMNUM', 'QMART', 'QMTXT', 'ARTPR', 'PRIOK', 'ERNAM', 'ERDAT', 'AENAM', 'AEDAT', 'MZEIT', 'QMDAT', 'QMNAM', 'STRMN', 'STRUR', 'LTRMN', 'LTRUR', 'WAERS', 'AUFNR', 'VERID']
        
        df = pd.DataFrame(cleaned_lines, columns=columns)
        logger.info(f"DataFrame shape after initial creation: {df.shape}")

        # Perform any additional cleaning or type conversions here
        # For example:
        df['ERDAT'] = pd.to_datetime(df['ERDAT'], errors='coerce')
        df['AEDAT'] = pd.to_datetime(df['AEDAT'], errors='coerce')
        df['QMDAT'] = pd.to_datetime(df['QMDAT'], errors='coerce')
        df['STRMN'] = pd.to_datetime(df['STRMN'], errors='coerce')
        df['LTRMN'] = pd.to_datetime(df['LTRMN'], errors='coerce')
        df['ARTPR'] = pd.to_numeric(df['ARTPR'], errors='coerce')
        df['PRIOK'] = pd.to_numeric(df['PRIOK'], errors='coerce')

        logger.info(f"Data cleaned. Shape after cleaning: {df.shape}")

        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Cleaned data saved to {output_file}")

        return df

    except Exception as e:
        logger.error(f"Error in clean_data function: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def validate_data(df):
    logger.info("Validating cleaned data")
    logger.info(f"Columns in the dataset: {df.columns.tolist()}")
    logger.info(f"Data types of columns:\n{df.dtypes}")
    logger.info(f"First few rows of the dataset:\n{df.head().to_string()}")
    logger.info(f"Summary statistics:\n{df.describe().to_string()}")
    logger.info(f"Missing values:\n{df.isnull().sum()}")
    # Add more validation checks as needed

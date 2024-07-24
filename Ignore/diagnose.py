# diagnose_input.py

import logging

logger = logging.getLogger('DataDiagnoser')

def diagnose_file(input_file):
    logger.info(f"Starting to diagnose file: {input_file}")
    
    try:
        # Read the file line by line
        with open(input_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        logger.info(f"Total lines read from file: {len(lines)}")

        # Print the first 10 lines of the file
        for i, line in enumerate(lines[:10]):
            logger.info(f"Line {i + 1}: {line.strip()}")

        # Identify lines with incorrect number of fields
        incorrect_lines = []
        for i, line in enumerate(lines[2:]):  # Skipping the first two lines
            if len(line.split('|')) != len(COLUMNS) + 1:  # +1 because of leading and trailing |
                incorrect_lines.append((i + 3, line.strip()))  # +3 to account for skipping lines and 0-based index

        logger.info(f"Total lines with incorrect number of fields: {len(incorrect_lines)}")

        # Print the first 10 incorrect lines
        for i, (line_num, line) in enumerate(incorrect_lines[:10]):
            logger.info(f"Incorrect Line {line_num}: {line}")

    except Exception as e:
        logger.error(f"Error diagnosing file: {str(e)}")
        raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    input_file = '/Users/fahadkiani/Desktop/development/celonis/input/raw.txt'
    diagnose_file(input_file)

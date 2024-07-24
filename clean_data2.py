import pandas as pd
import numpy as np

# Load the CSV file
file_path = '/Users/fahadkiani/Desktop/development/celonis/cleaned_data.csv'
claims_data = pd.read_csv(file_path)

# Define a threshold to drop columns or rows with too many missing values
threshold = 0.6

# Drop columns with more than 60% missing values
claims_data = claims_data.dropna(axis=1, thresh=int(threshold * len(claims_data)))

# Drop rows with more than 60% missing values
claims_data = claims_data.dropna(axis=0, thresh=int(threshold * len(claims_data.columns)))

# Fill missing numeric values with the mean of the column
numeric_columns = claims_data.select_dtypes(include=['float64', 'int64']).columns
claims_data[numeric_columns] = claims_data[numeric_columns].fillna(claims_data[numeric_columns].mean())

# Fill missing categorical values with the mode of the column
categorical_columns = claims_data.select_dtypes(include=['object']).columns
for column in categorical_columns:
    mode_value = claims_data[column].mode()[0]
    claims_data[column] = claims_data[column].fillna(mode_value)

# Handle date columns and replace invalid dates
date_columns = ['ERDAT', 'AEDAT', 'QMDAT', 'STRMN', 'LTRMN']
for col in date_columns:
    claims_data[col] = pd.to_datetime(claims_data[col], format='%d.%m.%Y', errors='coerce')

# Handle time columns
time_columns = ['MZEIT', 'STRUR', 'LTRUR']
for col in time_columns:
    claims_data[col] = pd.to_datetime(claims_data[col], format='%H:%M:%S', errors='coerce').dt.time

# Replace remaining invalid dates and times with NaT or NaN
claims_data[date_columns] = claims_data[date_columns].replace({pd.NaT: np.nan})
claims_data[time_columns] = claims_data[time_columns].replace({pd.NaT: np.nan})

# Convert all categorical columns to string before applying string operations
for column in categorical_columns:
    claims_data[column] = claims_data[column].astype(str).str.upper()

# Save the cleaned data to a new CSV file
cleaned_file_path = '/Users/fahadkiani/Desktop/development/celonis/processed/cleaned_processed_claims_data_comprehensive.csv'
claims_data.to_csv(cleaned_file_path, index=False)

print(f"Cleaned data saved to {cleaned_file_path}")

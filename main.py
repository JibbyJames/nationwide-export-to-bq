import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SchemaField
from datetime import datetime

# Define BigQuery dataset and table
PROJECT_ID = 'james-gcp-project'
DATASET_NAME = 'personal_finance'
TABLE_NAME = 'nationwide_exports'

# Set the path to the directory containing CSV files
CSV_DIRECTORY = 'exports'

# Path to service account key file
service_account_json = 'gcp_service_account.json'

# Initialize BigQuery client using the service account JSON key file
client = bigquery.Client.from_service_account_json(service_account_json)

# BigQuery table schema
schema = [
    SchemaField("account_name", "STRING"),
    SchemaField("date", "DATE"),
    SchemaField("transaction_type", "STRING"),
    SchemaField("description", "STRING"),
    SchemaField("paid_out", "FLOAT64"),
    SchemaField("paid_in", "FLOAT64"),
    SchemaField("balance", "FLOAT64"),
    SchemaField("date_uploaded", "DATE"),
]

# Load job configuration
job_config = LoadJobConfig(
    schema=schema,
    write_disposition=WriteDisposition.WRITE_APPEND
)


# Function to extract account name from summary
def extract_account_name(summary_row):
    return summary_row.split(',')[1].strip('"')


def currency_to_float(currency_series):
    # If the series is already in float format, return as is
    if currency_series.dtype == 'float64':
        return currency_series
    # Check if the series is of string type
    elif currency_series.dtype == 'object':
        # Remove non-numeric characters except the decimal point
        cleaned_series = currency_series.str.replace('[^0-9.]', '', regex=True)
        # Convert the cleaned strings to numeric values, coerce errors to NaN
        return pd.to_numeric(cleaned_series, errors='coerce')
    else:
        # Print the actual type and return the series unmodified
        print(f"The column data type is not a string or float. It's a {currency_series.dtype} type.")
        return currency_series



# Function to process each CSV file and upload to BigQuery
def process_csv_file(file_path):
    with open(file_path, 'r') as file:
        # Read the summary line to get the account name
        account_name = extract_account_name(file.readline())

    # Read the CSV data skipping the summary part
    data = pd.read_csv(file_path, skiprows=4)
    data['account_name'] = account_name
    data['date_uploaded'] = datetime.now().date()

    # Replace spaces with underscores and convert to lowercase
    data.columns = data.columns.str.replace(' ', '_').str.lower()

    # Convert the 'Paid out', 'Paid in', and 'Balance' columns to float
    data['paid_out'] = currency_to_float(data['paid_out'])
    data['paid_in'] = currency_to_float(data['paid_in'])
    data['balance'] = currency_to_float(data['balance'])

    # Convert the 'Date' column to datetime
    data['date'] = pd.to_datetime(data['date'], format='%d %b %Y')

    # Convert the DataFrame to a BigQuery-friendly format
    job = client.load_table_from_dataframe(data, f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}', job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f'Uploaded data from {file_path} to BigQuery')


def get_table_row_count():
    query = f"SELECT COUNT(*) as total FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`"
    count_result = client.query(query).result().to_dataframe().iloc[0, 0]
    return count_result

# Function to remove duplicates from the BigQuery table
def remove_duplicates():
    # Get the initial count of rows
    initial_count = get_table_row_count()

    # Query to find and delete duplicate rows, considering all columns for duplicates
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}` AS
    SELECT DISTINCT * FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`;
    """
    # Run the query
    job = client.query(query)
    job.result()  # Wait for the job to complete

    # Get the count of rows after removing duplicates
    final_count = get_table_row_count()

    duplicates_removed = initial_count - final_count

    print(f'{duplicates_removed} duplicate rows were removed.')


# Loop through all CSV files in the directory
for filename in os.listdir(CSV_DIRECTORY):
    if filename.endswith('.csv'):
        process_csv_file(os.path.join(CSV_DIRECTORY, filename))

print('All files have been processed and uploaded.')

# After all files have been processed and uploaded, remove duplicate rows
remove_duplicates()

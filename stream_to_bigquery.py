import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time

# --- Configuration ---
# IMPORTANT: Double-check that this is your correct, unique Project ID from the Google Cloud Console.
PROJECT_ID = "live-netflix-dashboard" 
DATASET_ID = "netflix_data"
TABLE_ID = "enriched_titles"
FILE_PATH = "netflix_enriched_final.csv"
DELAY = 2 # Delay between sending each row (in seconds)
# ---------------------

def setup_bigquery_table(client, table_ref):
    """Checks if the table exists and creates it with a defined schema if it doesn't."""
    try:
        client.get_table(table_ref)  # Make an API request.
        print(f"Table {TABLE_ID} already exists.")
    except NotFound:
        print(f"Table {TABLE_ID} not found, creating it with schema...")
        # Define the schema for your table.
        schema = [
            bigquery.SchemaField("show_id", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("director", "STRING"),
            bigquery.SchemaField("cast", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("date_added", "STRING"),
            bigquery.SchemaField("release_year", "INTEGER"),
            bigquery.SchemaField("rating", "STRING"),
            bigquery.SchemaField("duration", "STRING"),
            bigquery.SchemaField("listed_in", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("tmdb_vote_average", "FLOAT"),
            bigquery.SchemaField("tmdb_vote_count", "INTEGER"),
            bigquery.SchemaField("budget", "INTEGER"),
            bigquery.SchemaField("revenue", "INTEGER"),
            bigquery.SchemaField("popularity", "FLOAT"),
            bigquery.SchemaField("primary_country", "STRING"),
            bigquery.SchemaField("primary_genre", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # Make an API request.
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def stream_to_bigquery():
    """Reads a CSV and streams its rows one by one into a BigQuery table."""
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    # Step 1: Ensure the table exists before streaming.
    setup_bigquery_table(client, table_ref)

    # Step 2: Read the local data.
    print(f"Reading data from {FILE_PATH}...")
    try:
        # Replace pandas NaN values with None, which BigQuery understands as NULL.
        df = pd.read_csv(FILE_PATH).where(pd.notnull, None) 
    except FileNotFoundError:
        print(f"Error: The file '{FILE_PATH}' was not found.")
        return

    # Step 3: Start the streaming loop.
    print(f"Starting to stream {len(df)} rows to BigQuery...")
    print(f"One row will be sent every {DELAY} seconds. Press Ctrl+C to stop.")

    try:
        for index, row in df.iterrows():
            row_to_insert = [row.to_dict()]
            errors = client.insert_rows_json(table_ref, row_to_insert)
            
            if not errors:
                print(f"Sent: {row['title']}")
            else:
                print(f"Encountered errors while inserting rows: {errors}")
            
            time.sleep(DELAY)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    if "your-gcp-project-id" in PROJECT_ID:
        print("Error: Please update the PROJECT_ID variable in the script with your real Project ID.")
    else:
        stream_to_bigquery()
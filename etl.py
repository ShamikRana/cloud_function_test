# Minimal helpers: download CSV from GCS -> pandas, then load pandas -> BigQuery
from google.cloud import storage, bigquery
import pandas as pd
import io
from typing import Tuple

def get_data_from_gcs(bucket_name: str, file_name: str) -> pd.DataFrame:
    """
    Download a single CSV file from GCS and return a pandas DataFrame.

    Args:
        bucket_name: name of the GCS bucket (e.g. "my-bucket")
        file_name: path of the CSV inside the bucket (e.g. "incoming/data.csv")

    Returns:
        pandas.DataFrame
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content))
    print(f"Loaded {len(df)} rows from gs://{bucket_name}/{file_name}")
    return df


def load_df_to_bq(df: pd.DataFrame, project: str, dataset: str, table: str,
                  write_disposition: str = "WRITE_APPEND") -> Tuple[bool, int]:
    """
    Load a pandas DataFrame into BigQuery.

    Args:
        df: DataFrame to load
        project: GCP project id (e.g. "my-project")
        dataset: BigQuery dataset (e.g. "my_dataset")
        table: BigQuery table name (e.g. "my_table")
        write_disposition: one of "WRITE_APPEND" or "WRITE_TRUNCATE"

    Returns:
        (success: bool, rows_loaded: int)
    """
    if df is None or df.empty:
        print("DataFrame is empty — nothing to load.")
        return True, 0

    client = bigquery.Client(project=project)
    destination = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_disposition
    # Let BigQuery infer schema from DataFrame; if you need strict schema, set job_config.schema

    load_job = client.load_table_from_dataframe(df, destination, job_config=job_config)
    load_job.result()  # wait for completion
    rows = int(load_job.output_rows or len(df))
    print(f"Loaded {rows} rows into {destination}")
    return True, rows

# Transformation
def dp013(resolution_time):
    if resolution_time <= 1800:
        return 10
    elif 1800 < resolution_time <= 7200:
        return 7
    elif 7200 < resolution_time <= 10800:
        return 5
    else:
        return 0


# -----------------------
# Example usage (run in notebook or script)
# -----------------------
if __name__ == "__main__":
    BUCKET = "test_bucket_shamik"
    FILE = "User_Journey.csv"
    PROJECT = "corded-reality-475215-f8"
    DATASET = "test_dataset"
    TABLE = "test_table3"

    # 1) Read from GCS
    df = get_data_from_gcs(BUCKET, FILE)

    # Optional: do simple transform here
    df['DP013'] = df['calendar_duration'].apply(dp013)

    # 2) Load to BigQuery (append)
    success, rows = load_df_to_bq(df, PROJECT, DATASET, TABLE, write_disposition="WRITE_APPEND")
    print("Done:", success, rows)

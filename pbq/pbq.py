import os
import tempfile
import shutil
from google.cloud import bigquery
from google.cloud import storage as gcs
import pandas as pd
from util import random_string

tmp_file_name = "tmp_extract.gz"


def read_bq_with_sql(project_id, location, query, tmp_dataset_id, tmp_gcs_bucket):
    tmp_table_name = random_string(50)

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(tmp_dataset_id).table(tmp_table_name)
    job_config.destination = table_ref

    try:
        query_job = client.query(
            query,
            location=location,
            job_config=job_config)
        query_job.result()
        pd_data = read_bq(project_id, tmp_dataset_id, tmp_table_name, tmp_gcs_bucket)
    finally:
        table_ref = client.dataset(tmp_dataset_id).table(tmp_table_name)
        client.delete_table(table_ref)

    return pd_data


def read_bq(project_id, dataset_id, table_id, tmp_gcs_bucket):
    client = bigquery.Client()

    table = _setup_bq_table(client, project_id, dataset_id, table_id)
    random_file = _create_random_gcs_file()
    path = os.path.join("gs://", tmp_gcs_bucket, random_file)

    try:
        _extract_table(client, table, path)
        local_path = os.path.join(tempfile.mkdtemp(), tmp_file_name)
        _cp_file_from_gcs(tmp_gcs_bucket, random_file, local_path)
        pd_data = pd.read_csv(local_path, compression="gzip")
    finally:
        _delete_dir_from_gcs(tmp_gcs_bucket, random_file)
        if 'local_path' in locals():
            shutil.rmtree(os.path.dirname(local_path))

    return pd_data


def _setup_bq_table(client, project_id, dataset_id, table_id):
    dataset = client.dataset(dataset_id, project=project_id)
    table = dataset.table(table_id)
    return table


def _extract_table(client, table, path):
    config = _create_bq_config()

    job = client.extract_table(
        table,
        path,
        job_config=config
    )
    job.result()


def _create_bq_config():
    config = bigquery.ExtractJobConfig()
    config.compression = bigquery.Compression.GZIP
    config.destination_format = bigquery.DestinationFormat.CSV

    return config


def _create_random_gcs_file():
    random_dir = random_string(100)
    path = os.path.join(random_dir, tmp_file_name)

    return path


def _cp_file_from_gcs(bucket_name, gcs_path, local_path):
    client = gcs.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.download_to_filename(local_path)


def _delete_dir_from_gcs(bucket_name, gcs_path):
    client = gcs.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for b in blobs:
        if b.name == gcs_path:
            blob = bucket.blob(gcs_path)
            blob.delete()
            return

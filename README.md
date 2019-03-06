# PandasBQ

fast data extract function from BigQuery to Pandas DataFrame.  
The function is 8 times faster than pandas.read_gbq.

## interface
```
pbq.read_bq_with_sql(project_id, location, query, tmp_dataset_id, tmp_gcs_bucket)

get Pandas DataFrame from BigQuery

- project_id
  GCP project id

- location
  BigQuery location

- query
  SQL query with BigQuery syntax

- tmp_dataset_id
  BigQuery dataset id. Temporary table is created under the dataset id.

- tmp_gcs_bucket
  GCS bucket. Temporary GCS file is created under the GCS bucket.
```

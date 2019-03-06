import argparse
import time
from pbq.pbq import read_bq, read_bq_with_sql

parser = argparse.ArgumentParser(description="read_bq test")
parser.add_argument("--project_id")
parser.add_argument("--dataset_id")
parser.add_argument("--table_id")
parser.add_argument("--bucket")
parser.add_argument("--location")
parser.add_argument("--query")
args = parser.parse_args()

start = time.time()
df = read_bq_with_sql(args.project_id,
                      args.location,
                      args.query,
                      args.dataset_id,
                      args.bucket)
elapsed_time = time.time() - start

print(df.head())
print("elapsed_time:{}[sec]".format(elapsed_time))


start = time.time()
df = read_bq(args.project_id,
             args.dataset_id,
             args.table_id,
             args.bucket)
elapsed_time = time.time() - start

print(df.head())
print("elapsed_time:{}[sec]".format(elapsed_time))



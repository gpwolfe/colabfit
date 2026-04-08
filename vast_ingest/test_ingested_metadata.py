import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from colabfit.tools.database import (
    VAST_BUCKET_DIR,
    S3FileManager,
)

load_dotenv()

spark = SparkSession.builder.appName("oc20_missing").getOrCreate()
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")

s = S3FileManager(VAST_BUCKET_DIR, access_key, access_secret, endpoint)
pos = spark.table("ndb.colabfit.dev.po_wip")
cos = spark.table("ndb.colabfit.dev.co_wip")
# Look at a row of pos or cos, find metadata_path and paste into function below
s.read_file()

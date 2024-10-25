import os
import pickle
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark


print("pyspark version: ", pyspark.__version__)
# Set up data loader environment
load_dotenv()
n_cpus = 1
spark_ui_port = 31111
jars = jars = os.getenv("VASTDB_CONNECTOR_JARS")
print(jars)
spark_session = (
    SparkSession.builder.appName("minimal_test")
    .master(f"local[{n_cpus}]")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
    .getOrCreate()
)

print(spark_session.table("ndb.colabfit.dev.po_wip").columns)

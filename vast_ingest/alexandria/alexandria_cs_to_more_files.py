from colabfit.tools.schema import *

import json
import os
from itertools import islice
from pathlib import Path
from pickle import dump, load
from time import time

import numpy as np
import pyspark
import pyspark.sql.functions as sf
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.configuration_set import ConfigurationSet
from colabfit.tools.database import DataManager, VastDataLoader, batched
from colabfit.tools.property import PropertyMap, property_info
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.utilities import unstring_df_val
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tqdm import tqdm

with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))

n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("colabfit")
    .config("spark.jars", jars)
    .config("spark.sql.shuffle.partitions", 2400)
    .getOrCreate()
)

loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_spark_session(spark)
access_key = envvars.get("SPARK_ID")
access_secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")

path_prefix = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alexandria_3d_pbe_traj/alexandria_css/"
)
dir_path_by_cs_id = Path("/vast/gw2338/alexandria_cs_co2")


# paths = list(path_prefix.glob("*alex_go_aak*"))
# paths.extend(list(path_prefix.glob("*alex_go_aao*")))
dir_path = sorted(list(path_prefix.glob("*")))[SLURM_TASK_ID]
# paths = sorted(paths)
# dir_path = paths[SLURM_TASK_ID]

dirname = dir_path.name
css = spark.read.csv(
    str(dir_path),
    schema=StructType(
        [
            StructField("cs_name", StringType(), True),
            StructField("config_id", StringType(), True),
        ]
    ),
)
cs_names = [x[0] for x in css.select("cs_name").distinct().collect()]
batches = batched(cs_names, 100)
for i, batch in tqdm(enumerate(batches)):
    towrite = css.filter(sf.col("cs_name").isin(batch))
    write_dir = str(dir_path_by_cs_id / dirname / f"configuration_set_{i}")
    towrite.write.parquet(write_dir)

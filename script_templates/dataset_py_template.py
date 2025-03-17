"""
author: Gregory Wolfe
"""

import os
from time import time

from colabfit.tools.vast.database import DataManager, VastDataLoader
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID", -1))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID", -1)
ACTUAL_INDEX = SLURM_TASK_ID

spark_session = SparkSession.builder.appName(
    f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}"
).getOrCreate()

loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_spark_session(spark_session)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID", -1))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID", -1)
ACTUAL_INDEX = SLURM_TASK_ID

spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.driver.memory", "12g")
    .config("spark.ui.showConsoleProgress", "true")
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.rpc.message.maxSize", "2047")
    .getOrCreate()
)

loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_spark_session(spark_session)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)
loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_NAME = "NAME"
DATASET_ID = "ID"
DESCRIPTION = "DESCRIPTION"


DOI = None
PUBLICATION_YEAR = "2024"
AUTHORS = [
    "author1",
    "author2",
]

LICENSE = "CC-BY-4.0"
PUBLICATION = "EXAMPLE"
DATA_LINK = "EXAMPLE"
OTHER_LINKS = None


def main():
    t0 = time()
    dm = DataManager(
        dataset_id=DATASET_ID,
    )
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        other_links=OTHER_LINKS,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
        # labels=labels,
    )
    print(f"Time to create dataset: {time() - t}")
    print(f"Total time: {time() - t0}")
    print("complete")


if __name__ == "__main__":
    main()

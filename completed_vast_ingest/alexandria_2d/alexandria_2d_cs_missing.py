import logging
import os
import sys
from pathlib import Path
from time import time

import pyspark.sql.functions as sf
from colabfit.tools.vast.configuration_set import ConfigurationSet
from colabfit.tools.vast.database import VastDataLoader
from colabfit.tools.vast.schema import (
    co_cs_map_schema,
    config_schema,
    configuration_set_arr_schema,
    configuration_set_schema,
)
from colabfit.tools.vast.utilities import (
    str_to_arrayof_int,
    str_to_arrayof_str,
    stringify_df_val_udf,
)
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType
from tqdm import tqdm


logger = logging.getLogger(__name__)
logger.setLevel("INFO")
with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())


SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
    .config("spark.task.maxFailures", 1)
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

missing = [
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_7.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_70.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_71.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_72.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_73.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_74.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_75.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_76.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_77.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_78.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_79.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_8.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_80.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_81.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_82.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_83.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_000_9.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_0.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_1.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_10.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_11.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_12.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_13.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_14.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_15.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_16.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_17.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_18.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_36.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_37.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_38.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_39.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_40.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_41.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_42.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_43.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_44.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_45.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_46.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_47.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_48.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_49.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_5.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_50.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_51.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_52.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_53.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_54.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_55.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_56.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_57.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_58.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_59.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_6.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_60.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_61.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_62.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_63.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_64.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_65.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_66.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_67.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_68.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_69.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_7.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_70.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_71.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_72.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_73.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_74.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_75.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_76.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_77.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_78.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_79.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_8.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_80.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_81.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_82.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_83.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_001_9.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_0.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_1.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_10.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_11.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_12.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_13.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_14.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_15.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_16.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_17.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_18.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_002_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_36.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_37.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_38.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_39.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_40.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_41.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_42.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_43.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_44.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_45.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_46.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_47.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_48.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_49.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_5.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_50.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_51.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_52.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_53.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_54.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_55.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_56.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_57.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_58.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_59.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aab_003_6.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_14.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_15.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_16.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_17.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_18.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_36.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_37.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_38.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_39.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_000_40.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_36.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_37.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_38.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_39.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_001_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_002_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_003_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_104.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_105.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_106.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_107.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_11.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_12.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_13.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_14.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_15.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_16.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_17.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_18.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_004_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aac_005_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_11.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_12.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_13.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_14.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_15.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_16.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_17.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_18.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_000_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_36.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_37.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_38.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_39.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_40.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_41.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_42.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_43.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_44.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_45.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_46.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_47.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_48.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_49.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_5.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_50.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_51.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_52.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_53.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_001_54.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_31.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_32.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_33.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_34.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_35.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_36.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_37.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_38.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_39.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_40.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_41.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_42.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_43.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_44.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_45.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_46.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_47.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_48.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_49.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_5.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_50.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_51.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_52.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_53.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_6.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_7.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_8.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_002_9.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_0.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_1.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_10.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_11.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_12.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_13.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_14.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_15.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_16.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_17.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_18.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_19.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_2.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_20.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_21.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_22.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_23.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_24.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_25.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_26.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_27.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_28.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_29.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_3.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_30.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_4.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_5.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_6.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_7.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_8.csv",
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex2d/alexandria_2d_css/alexandria_2d_alex_go_aae_003_9.csv",
]


def main(start, end):
    loader = VastDataLoader(
        table_prefix="ndb.colabfit.dev",
    )
    loader.set_spark_session(spark)
    access_key = envvars.get("SPARK_ID")
    access_secret = envvars.get("SPARK_KEY")
    endpoint = envvars.get("SPARK_ENDPOINT")

    loader.set_vastdb_session(
        endpoint=endpoint,
        access_key=access_key,
        access_secret=access_secret,
    )
    loader.config_set_table = "ndb.colabfit.dev.cs_alex"
    loader.config_table = "ndb.colabfit.dev.co_alex"
    loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_alex"
    fps = [Path(x) for x in missing]
    if start > len(fps):
        raise ValueError("Start index is greater than the number of files.")
    if end > len(fps):
        end = len(fps)
    fps = fps[start:end]

    def make_desc(cs_name):
        ds_name, filename, mat_id, traj = cs_name.split("__")
        filename = filename.replace("file_", "")
        mat_id = mat_id.replace("id_", "")
        traj = traj.replace("trajectory_", "")
        desc = f"Alexandria 2D PBE trajectory number {traj} for material {mat_id} from file {filename}"  # noqa E501
        return desc

    for f in fps:
        css = (
            spark.read.option("header", "true")
            .csv(str(f))
            .drop_duplicates()
            .where('config_id != "config_id"')
        )
        cs_names = set([x[0] for x in css.select("cs_name").collect()])
        co_ids = set([x[0] for x in css.select("config_id").collect()])
        logger.info(f"# num CO IDs  : {len(co_ids)}")
        logger.info(f"# num CS names: {len(cs_names)}")

        config_df_cols = [
            "id",
            "nsites",
            "elements",
            "nperiodic_dimensions",
            "dimension_types",
            "atomic_numbers",
            "names",
            "dataset_ids",
        ]
        begin = time()
        with loader.session.transaction() as tx:
            table = tx.bucket("colabfit").schema("dev").table("co_alex")
            reader = table.select(
                predicate=table["id"].isin(co_ids),
                columns=config_df_cols,
                internal_row_id=True,
            )
            t2 = time()
            logger.info(f"selected in {t2 - begin}")
            config_batch = reader.read_all()
            t3 = time()
            logger.info(f"{len(config_batch)}, {t3 - t2}")
            configs = config_batch.to_struct_array().to_pandas()
            logger.info(f"configs {time() - t3}")
        logger.info(f"total {time() - begin}")

        read_schema = StructType(
            [field for field in config_schema.fields if field.name in config_df_cols]
        )
        configs = spark.createDataFrame(configs, schema=read_schema).drop_duplicates(
            ["id"]
        )
        configs = configs.join(css, on=css.config_id == configs.id, how="outer").drop(
            "config_id"
        )
        string_cols = [
            "elements",
        ]
        for col in string_cols:
            configs = configs.withColumn(col, str_to_arrayof_str(sf.col(col)))
        int_cols = [
            "atomic_numbers",
            "dimension_types",
        ]
        for col in int_cols:
            configs = configs.withColumn(col, str_to_arrayof_int(sf.col(col)))
        # break

        logger.info(f"config_table: {loader.config_table}")
        configs.cache()
        begin = time()
        cs_rows = []
        for name in tqdm(cs_names):
            desc = make_desc(name)
            co_batch = configs.filter(sf.col("cs_name") == name)
            cs = ConfigurationSet(
                config_df=co_batch,
                name=name,
                description=desc,
                dataset_id="DS_6pieq95jrqpn_0",
            )
            cs.row_dict["ordered"] = True
            cs.row_dict["extended_id"] = cs.id
            cs_rows.append(cs.row_dict)

        # # About 1.3 seconds per iter for creating the CS without updating COs
        logger.info(f"Time to create css from file: {time() - begin}")
        logger.info("Creating CS dataframe")
        new_cs_df = spark.createDataFrame(
            [Row(**row_dict) for row_dict in cs_rows],
            schema=configuration_set_arr_schema,
        )
        arr_cols = [
            col.name
            for col in configuration_set_arr_schema
            if col.dataType.typeName() == "array"
        ]
        new_cs_df2 = new_cs_df.select(
            [
                (
                    col
                    if col not in arr_cols
                    else stringify_df_val_udf(sf.col(col)).alias(col)
                )
                for col in configuration_set_schema.names
            ]
        )

        logger.info("Creating and writing CS-CO map dataframe")
        cs_co_map_df = (
            css.withColumnRenamed("config_id", "configuration_id")
            .join(
                new_cs_df.select("name", "id").withColumnRenamed(
                    "id", "configuration_set_id"
                ),
                on=new_cs_df.name == css.cs_name,
                how="left",
            )
            .drop("name", "cs_name")
        ).drop_duplicates()
        assert cs_co_map_df.schema == co_cs_map_schema

        logger.info("writing cs to table")
        new_cs_df2.write.mode("append").saveAsTable(loader.config_set_table)
        logger.info("writing cs-co map to table")
        cs_co_map_df.write.mode("append").saveAsTable(loader.co_cs_map_table)
        configs.unpersist()
        logger.info(f"Done with file {f}")

    logger.info("Finished!")
    logger.info(f"Time to finish: {time() - begin}")
    spark.stop()


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) != 2:
        print("Usage: python script.py <start_index> <end_index>")
        sys.exit(1)
    start = int(args[0])
    end = int(args[1])
    logger.info(f"indexes {start} to {end}")
    main(start, end)

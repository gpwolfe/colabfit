import os
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

with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())


ACTUAL_INDEX = int(os.getenv("ACTUAL_INDEX"))
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
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
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.config_table = "ndb.colabfit.dev.co_wip"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"


cs_fps = spark.read.parquet("/vast/gw2338/alexandria_config_set_filepaths_indexed")
cs_fp = (
    cs_fps.filter(sf.col("index") == ACTUAL_INDEX).select("config_set_fps").first()[0]
)
print(f"\n# File index: {ACTUAL_INDEX}")
print(f"\n# File: {cs_fp}\n")

css = spark.read.parquet(cs_fp).drop_duplicates().where('config_id != "config_id"')
cs_names = set([x[0] for x in css.select("cs_name").collect()])
co_ids = set([x[0] for x in css.select("config_id").collect()])
print("# num CO IDs  : ", len(co_ids))
print("# num CS names: ", len(cs_names))

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
    table = tx.bucket("colabfit").schema("dev").table("co_wip")
    reader = table.select(
        predicate=table["id"].isin(co_ids), columns=config_df_cols, internal_row_id=True
    )
    t2 = time()
    print("selected in ", t2 - begin)
    config_batch = reader.read_all()
    t3 = time()
    print(len(config_batch), t3 - t2)
    configs = config_batch.to_struct_array().to_pandas()
    print("configs", time() - t3)
print("total", time() - begin)

read_schema = StructType(
    [field for field in config_schema.fields if field.name in config_df_cols]
)
configs = spark.createDataFrame(configs, schema=read_schema).drop_duplicates(["id"])
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


def make_desc(cs_name):
    ds_name, filename, mat_id, traj = cs_name.split("__")
    filename = filename.replace("file_", "")
    mat_id = mat_id.replace("id_", "")
    traj = traj.replace("trajectory_", "")
    desc = f"Alexandria 3D PBE trajectory number {traj} for material {mat_id} from file {filename}"  # noqa E501
    return desc


###################################################################
# Begin creating the configuration sets and updating the tables  ##
###################################################################

print("config_table: ", loader.config_table)
configs.cache()
begin = time()
cs_rows = []
for name in tqdm(cs_names):
    desc = make_desc(name)
    co_batch = configs.filter(sf.col("cs_name") == name)
    cs = ConfigurationSet(
        config_df=co_batch, name=name, description=desc, dataset_id="DS_s6gf4z2hcjqy_0"
    )
    cs.row_dict["ordered"] = True
    cs.row_dict["extended_id"] = cs.id
    cs_rows.append(cs.row_dict)

# # About 1.3 seconds per iter for creating the CS without updating COs
print("total time: ", time() - begin)

print("Creating CS dataframe")
new_cs_df = spark.createDataFrame(
    [Row(**row_dict) for row_dict in cs_rows], schema=configuration_set_arr_schema
)
arr_cols = [
    col.name
    for col in configuration_set_arr_schema
    if col.dataType.typeName() == "array"
]
new_cs_df2 = new_cs_df.select(
    [
        col if col not in arr_cols else stringify_df_val_udf(sf.col(col)).alias(col)
        for col in configuration_set_schema.names
    ]
)

print("Creating and writing CS-CO map dataframe")
cs_co_map_df = (
    css.withColumnRenamed("config_id", "configuration_id")
    .join(
        new_cs_df.select("name", "id").withColumnRenamed("id", "configuration_set_id"),
        on=new_cs_df.name == css.cs_name,
        how="left",
    )
    .drop("name", "cs_name")
).drop_duplicates()
assert cs_co_map_df.schema == co_cs_map_schema

print("writing cs to table")
new_cs_df2.write.mode("append").saveAsTable(loader.config_set_table)
print("writing cs-co map to table")
cs_co_map_df.write.mode("append").saveAsTable(loader.co_cs_map_table)
configs.unpersist()
spark.stop()
print("Finished!")

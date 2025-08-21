import os
from pathlib import Path

from ase.io import iread
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration

# from colabfit.tools.vast.configuration_set import configuration_set_info
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from pyspark.sql import SparkSession
from vastdb.session import Session

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .getOrCreate()
)


with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

access_key = envvars.get("SPARK_ID")
access_secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

loader = VastDataLoader(
    spark_session=spark,
    access_key=envvars.get("SPARK_ID"),
    access_secret=envvars.get("SPARK_KEY"),
    endpoint=envvars.get("SPARK_ENDPOINT"),
)

loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_temp"
loader.config_set_table = "ndb.colabfit.dev.cs_temp"
loader.dataset_table = "ndb.colabfit.dev.ds_temp"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_temp"


DATASET_FP = Path("")
DATASET_NAME = ""
DATASET_ID = ""
DESCRIPTION = "A brief description"
PUBLICATION = ""
DATA_LINK = ""
OTHER_LINKS = []
PUBLICATION_YEAR = "2025"


AUTHORS = [""]
LICENSE = "CC-BY-4.0"
DOI = None
property_keys = []
property_map = PropertyMap([energy_pd, cauchy_stress_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field("input", {"energy_cutoff": "1000 eV"})
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)

energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="in header",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)
stress_info = property_info(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info])

PROPERTY_MAP = property_map.get_property_map()
co_md_keys = []
CO_MD_MAP = {key: {"field": key} for key in co_md_keys}


def reader(fp: Path):
    for atoms in iread(fp, format="extxyz"):
        yield AtomicConfiguration.from_ase(atoms)


dm = DataManager(
    prop_defs=[
        energy_pd,
        atomic_forces_pd,
        cauchy_stress_pd,
    ],
    configs=reader(DATASET_FP),
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=50000,
)


# CSS = [
#     configuration_set_info(
#         co_name_match="",
#         cs_name="",
#         cs_description="",
#         co_label_match=None,
#         ordered=False,
#     )
# ]


# print(dm.co_po_example_rows())

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

# dm.create_configuration_sets(loader, CSS)

dm.create_dataset(
    loader=loader,
    name=DATASET_NAME,
    authors=AUTHORS,
    publication_link=PUBLICATION,
    data_link=DATA_LINK,
    other_links=OTHER_LINKS,
    description=DESCRIPTION,
    data_license=LICENSE,
    publication_year=PUBLICATION_YEAR,
)

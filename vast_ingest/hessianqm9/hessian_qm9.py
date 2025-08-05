import logging
import os
from pathlib import Path
from pickle import load

from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd
from colabfit.tools.vast.configuration_set import configuration_set_info
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)


jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .config("spark.task.maxFailures", 1)
    .getOrCreate()
)

access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

loader = VastDataLoader(
    spark_session=spark,
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)

# loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_hessian_qm9"
loader.config_set_table = "ndb.colabfit.dev.cs_hessian_qm9"
loader.dataset_table = "ndb.colabfit.dev.ds_hessian_qm9"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_hessian_qm9"


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/hessian_qm9/pickles"
)
DATASET_NAME = "Hessian_QM9"
DATASET_ID = "DS_gk9tv5a9498z_0"
DESCRIPTION = "Hessian QM9 is the first database of equilibrium configurations and numerical Hessian matrices, consisting of 41,645 molecules from the QM9 dataset at the wB97x/6-31G* level. Molecular Hessians were calculated in vacuum, as well as in water, tetrahydrofuran, and toluene using an implicit solvation model."  # noqa: E501
PUBLICATION = "https://doi.org/10.1038/s41597-024-04361-2"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.26363959.v4"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Nicholas J. Williams",
    "Lara Kabalan",
    "Ljiljana Stojanovic",
    "Viktor Zólyomi",
    "Edward O. Pyzer-Knapp",
]
LICENSE = "CC0"
DOI = None
property_keys = [
    "frequencies",
    "normal_modes",
    "hessian",
    "label",
]


property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "NWChem")
property_map.set_metadata_field("method", "DFT-wB97x")
property_map.set_metadata_field("basis_set", "6-31G*")
property_map.set_metadata_field("input", {"energy_cutoff": "1e-6 eV"})
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)


energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
atomic_forces_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)

property_map.set_properties([energy_info, atomic_forces_info])
PROPERTY_MAP = property_map.get_property_map()


def reader(fp):
    with open(fp, "rb") as f:
        data = load(f)
    return data


def read_wrapper(fp_dir):
    for fp in fp_dir.rglob("*.pickle"):
        yield from reader(fp)


dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd],
    configs=read_wrapper(DATASET_FP),
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=30000,
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)
totalcount = spark.table(loader.config_table).count()
distcount = spark.table(loader.config_table).select("hash").distinct().count()
logger.info(
    f"Total count: {totalcount}, Distinct count: {distcount}, duplicates: {totalcount - distcount}"  # noqa: E501
)

css = [
    configuration_set_info(
        co_label_match=None,
        co_name_match="_water__",
        cs_description=f"Configurations from the water split of {DATASET_NAME}",
        cs_name=f"{DATASET_NAME}_water",
        ordered=True,
    ),
    configuration_set_info(
        co_label_match=None,
        co_name_match="_toluene__",
        cs_description=f"Configurations from the toluene split of {DATASET_NAME}",
        cs_name=f"{DATASET_NAME}_toluene",
        ordered=True,
    ),
    configuration_set_info(
        co_label_match=None,
        co_name_match="_vacuum__",
        cs_description=f"Configurations from the vacuum split of {DATASET_NAME}",
        cs_name=f"{DATASET_NAME}_vacuum",
        ordered=True,
    ),
    configuration_set_info(
        co_label_match=None,
        co_name_match="_thf__",
        cs_description=f"Configurations from the thf split of {DATASET_NAME}",
        cs_name=f"{DATASET_NAME}_thf",
        ordered=True,
    ),
]
logger.info("Creating configuration sets")
dm.create_configuration_sets(loader, css)
logger.info("creating dataset")
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
logger.info("Finished")

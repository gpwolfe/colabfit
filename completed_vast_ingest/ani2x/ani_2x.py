import logging
import os
from pathlib import Path
from pickle import load

from colabfit.tools.property_definitions import energy_pd
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)
TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
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

loader.config_table = "ndb.colabfit.dev.co_ani2x2"
loader.config_set_table = "ndb.colabfit.dev.cs_ani2x"
loader.dataset_table = "ndb.colabfit.dev.ds_ani2x"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_ani2x"


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/ani2x/pickles/"
)
DATASET_NAME = "ANI-2x-wB97X-def2TZVPP"
DATASET_ID = "DS_5bwpr2n9zxz9_0"
DESCRIPTION = "ANI-2x-wB97X-def2TZVPP is a portion of the ANI-2x dataset, which includes DFT-calculated energies for structures from 2 to 63 atoms in size containing H, C, N, O, S, F, and Cl. This portion of ANI-2x was calculated in ORCA at the wB97X level of theory using the def2TZVPP basis set. Configuration sets are divided by number of atoms per structure. Dipoles are recorded in the metadata."  # noqa: E501
PUBLICATION = "https://doi.org/10.1021/acs.jctc.0c00121"
DATA_LINK = "https://doi.org/10.5281/zenodo.10108942"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Christian Devereux",
    "Justin S. Smith",
    "Kate K. Huddleston",
    "Kipton Barros",
    "Roman Zubatyuk",
    "Olexandr Isayev",
    "Adrian E. Roitberg",
]
LICENSE = "CC-BY-4.0"
DOI = None
property_map = PropertyMap([energy_pd])

property_map.set_metadata_field("software", "ORCA 4.2.1")
property_map.set_metadata_field("method", "DFT-wB97X")
property_map.set_metadata_field("basis_set", "def2TZVPP")
property_map.set_metadata_field(
    "input", {"software_keywords": "! wb97x def2-tzvpp def2/j rijcosx engrad"}
)
property_keys = [
    "mbis_atomic_charges",
    "mbis_atomic_dipole_magnitudes",
    "mbis_atomic_volumes",
    "mbis_atomic_octupole_magnitudes",
    "mbis_atomic_quadrupole_magnitudes",
    "dipoles",
]
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)

energy_info = property_info(
    property_name="energy",
    field="energy",
    units="hartree",
    original_file_key="energies",
    additional=[("per-atom", {"value": False, "units": None})],
)
property_map.set_properties([energy_info])
PROPERTY_MAP = property_map.get_property_map()
PROPERTY_MAP


CSS = []


def read_pickle(fp):
    with fp.open("rb") as f:
        data = load(f)
        logger.info(f"Read {fp.name} with {len(data)} configurations.")
    return data


def read_wrapper(data_dir):
    data_dir = Path(data_dir)
    fps = sorted(list(data_dir.glob("*.pickle")))
    i = TASK_ID
    fp = fps[i]
    # for i, fp in enumerate(fps):
    logger.info(f"Reading file {i}: {fp}")
    yield from read_pickle(fp)
    logger.info(f"Finished file {i}: {fp}")


dm = DataManager(
    prop_defs=[energy_pd],
    configs=read_wrapper(DATASET_FP),
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=50000,
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

# with open(DATASET_FP.parent / "ani2x_config_set_map.pickle", "rb") as f:
#     css = load(f)
# dm.create_configuration_sets(loader, css)

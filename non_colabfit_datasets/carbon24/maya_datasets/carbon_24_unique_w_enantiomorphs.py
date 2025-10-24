import os

from ase.io import iread
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap
from pyspark.sql import SparkSession
from vastdb.session import Session

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .getOrCreate()
)

loader = VastDataLoader(
    spark_session=spark,
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
# from colabfit.tools.vast.utils.data_processing import generate_ds_id

# generate_ds_id()
# DS_ll0wuxuhe5g9_0
loader.config_table = "ndb.colabfit.dev.co_c24enant"
loader.config_set_table = "ndb.colabfit.dev.cs_c24enant"
loader.dataset_table = "ndb.colabfit.dev.ds_c24enant"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_c24enant"

DATASET_NAME = "Carbon-24_Unique_with_Enantiomorphs"  # noqa: E501
DATASET_ID = "DS_ll0wuxuhe5g9_0"
DESCRIPTION = "This dataset is a companion dataset to Carbon-24 Unique, containing enantiomorph pairs discovered within the Carbon-24 dataset. Carbon-24_Unique_with_Enantiomorphs has been cultivated from Carbon-24 (Pickard 2020, doi: 10.24435/materialscloud:2020.0026/v1). Contains 4,330 entries of unique carbon structures, where enantiomorphs are treated as distinct. The metadata column indicates the index of the respective enantiomorph pair, if any, as well as the original id from Carbon-24."  # noqa: E501
PUBLICATION = "https://doi.org/10.48550/arXiv.2509.12178"
DATA_LINK = None
OTHER_LINKS = ["https://archive.materialscloud.org/records/ajs8r-a2755"]
PUBLICATION_YEAR = "2025"

AUTHORS = [
    "Maya M. Martirossyan",
    "Thomas Egg",
    "Philipp Hoellmer",
    "George Karypis",
    "Mark Transtrum",
    "Adrian Roitberg",
    "Mingjie Liu",
    "Richard G. Hennig",
    "Ellad B. Tadmor",
    "Stefano Martiniani",
]
LICENSE = "CC-BY-4.0"
DOI = None

property_map = PropertyMap()
property_map.set_metadata_field("software", "CASTEP")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input", "Available details: https://doi.org/10.24435/materialscloud:2020.0026/v1"
)
for key in [
    "original_id",
    "index",
    "enantiomorph_pair_index",
]:
    property_map.set_metadata_field(key, key, dynamic=True)

PROPERTY_MAP = property_map.get_property_map()
print(PROPERTY_MAP)


def reader(xyz, csv_file):
    with open(csv_file, "r") as f:
        orig_ids = f.read().splitlines()
    for i, atoms in enumerate(iread(xyz, format="extxyz")):
        atoms.info["original_id"] = orig_ids[i]
        atoms.info["index"] = i
        atoms.info["enantiomorph_pair_index"] = atoms.info["enantiomorph"]
        atoms.info["_name"] = f"{DATASET_NAME}_index_{i}"
        yield AtomicConfiguration.from_ase(atoms)


gen = reader(
    "carbon_24_unique_withenants/carbon_24_unique_withenants_fixed_header.xyz",
    "carbon_24_unique_withenants/carbon_24_unique_withenants_materialsID.csv",
)


dm = DataManager(
    prop_defs=None,
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=False,
    read_write_batch_size=10000,
)
dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)
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
    equilibrium=False,
    date_requested="2025-05-19",
)

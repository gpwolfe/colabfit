"""
h5 file keys mostly only two levels deep. last key has 3 levels.
properties include energy and forces only
keys at final level are coordinates, forces, energy, cell and species

"""

import logging
import os
from pathlib import Path
from pickle import load

from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    energy_pd,
)
from colabfit.tools.vast.database import (
    DataManager,
    VastDataLoader,
)
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

# from colabfit.tools.vast.configuration_set import configuration_set_info

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


loader.config_table = "ndb.colabfit.dev.co_anixnr"
loader.config_set_table = "ndb.colabfit.dev.cs_anixnr"
loader.dataset_table = "ndb.colabfit.dev.ds_anixnr"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_anixnr"


DATASET_FP = Path("pickles")
DATASET_NAME = "ANI-1xnr"
DATASET_ID = "DS_elj5dyxwg3mw_0"
DESCRIPTION = "ANI-1xnr was developed to train the ANI-1xnr model, intended to model reactive chemistry. Specifically, ANI-1xnr is meant to represent carbon solid-phase nucleation, graphene ring formation from acetylene, biofuel additives, combustion of methane and the spontaneous formation of glycine from early earth small molecules. The dataset was generated using an active learning method in which ab initio nanoreactor simulations supplied MLIP training; the MLIP was subsequently tested and new simulations were generated based on structures tested with high uncertainty to supply the next cycle of MLIP training."  # noqa: E501
PUBLICATION = "https://doi.org/10.1038/s41557-023-01427-3"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.22814579"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"


AUTHORS = [
    "Shuhao Zhang",
    "Małgorzata Z. Makoś",
    "Ryan B. Jadrich",
    "Elfi Kraka",
    "Kipton Barros",
    "Benjamin T. Nebgen",
    "Sergei Tretiak",
    "Olexandr Isayev",
    "Nicholas Lubbers",
    "Richard A. Messerly",
    "Justin S. Smith",
]
LICENSE = "CC-BY-4.0"
DOI = None

property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "CP2K")
property_map.set_metadata_field("method", "KS-DFT-BLYP+D3")
property_map.set_metadata_field("basis_set", "TZV2P")
property_map.set_metadata_field(
    "input",
    {
        "plane-wave-cutoff": "600 Ry",
        "gaussian-contribution-cutoff": "60 Ry",
        "pseudopotential": "GTH",
    },
)


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
    read_write_batch_size=10000,
)
dm.load_co_po_to_vastdb(
    loader=loader,
    batching_ingest=True,
    check_existing=False,
)

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

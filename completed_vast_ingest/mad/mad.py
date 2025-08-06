"""
To pickle the xyz files from the MAD dataset

example header
Lattice="8.9435953806243 0.0 0.0 -2.6069227862562e-07 9.3966938824691 0.0 0.0 0.0 31.237382780000008" Properties=species:S:1:pos:R:3:forces:R:3 subset=MC2D split=train energy=-79.17844101743322 pbc="T T F" # noqa: E501
keys = [
"energy",
"subset",
"split",
"pbc",
"forces"]

forces will appear in arrays or calc
pbc is defined and not always all true or all false

"""

import logging
import os
from pathlib import Path

from ase.io import iread
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)
# TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
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

loader.config_table = "ndb.colabfit.dev.co_mad"
loader.config_set_table = "ndb.colabfit.dev.cs_mad"
loader.dataset_table = "ndb.colabfit.dev.ds_mad"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_mad"


logger = logging.getLogger(__name__)


DATASET_FP = Path("data")
DATASET_NAME = "Massive_Atomic_Diversity_MAD"
DATASET_ID = "DS_h8s4lfyits34_0"
DESCRIPTION = "From the creators: Starting from relatively small sets of stable structures, the dataset is built to contain “massive atomic diversity” (MAD) by aggressively distorting these configurations, with near-complete disregard for the stability of the resulting configurations. The electronic structure details, on the other hand, are chosen to maximize consistency rather than to obtain the most accurate prediction fora given structure, or to minimize computational effort. The MAD dataset we present here, despite containing fewer than 100k structures, has already been shown to enable training universal interatomic potentials that are competitive with models trained on traditional datasets with two to three orders of magnitude more structures."  # noqa: E501
PUBLICATION = "https://doi.org/10.48550/arXiv.2506.19674"
DATA_LINK = "https://doi.org/10.24435/materialscloud:vd-e8"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Arslan Mazitov",
    "Sofiia Chorna",
    "Guillaume Fraux",
    "Marnik Bercx",
    "Giovanni Pizzi",
    "Sandip De",
    "Michele Ceriotti",
]
LICENSE = "CC-BY-4.0"
DOI = None
property_keys = ["subset", "split", "free_energy", "dataset"]
property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])
property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-PBEsol")
property_map.set_metadata_field("basis_set", "def2TZVPP")
property_map.set_metadata_field(
    "input", {"plane-wave-cutoff": "110 Ry", "charge-density-cutoff": "1320 Ry"}
)
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)


energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
stress_info = property_info(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
atomic_forces_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)
property_map.set_properties([energy_info, stress_info, atomic_forces_info])
PROPERTY_MAP = property_map.get_property_map()
PROPERTY_MAP

# DS_h8s4lfyits34_0 MAD train
# DS_s05lflv67rsn_0 MAD test
# DS_xsog5nx0do2h_0 MAD val
# DS_x44bl0q0dyxr_0 mad-bench-mptrj-settings.xyz
# DS_dc4lwyrm55p4_0 mad-bench-mad-settings.xyz
DSS = {
    "mad-train.xyz": {
        "id": "DS_h8s4lfyits34_0",
        "desc": f"The training split of the MAD (Massive Atomic Diversity) dataset. {DESCRIPTION}",  # noqa: E501
        "name": f"{DATASET_NAME}_train",
    },
    "mad-test.xyz": {
        "id": "DS_s05lflv67rsn_0",
        "desc": f"The test split of the MAD (Massive Atomic Diversity) dataset. {DESCRIPTION}",  # noqa: E501
        "name": f"{DATASET_NAME}_test",
    },
    "mad-val.xyz": {
        "id": "DS_xsog5nx0do2h_0",
        "desc": f"The validation split of the MAD (Massive Atomic Diversity) dataset. {DESCRIPTION}",  # noqa: E501
        "name": f"{DATASET_NAME}_val",
    },
    "mad-bench-mptrj-settings.xyz": {
        "id": "DS_x44bl0q0dyxr_0",
        "desc": f"The MAD benchmark dataset, containing a selection of MAD test, MPtrj, Alexandria, SPICE, MD22 and OC2020 datasets, computed with MPtrj DFT settings. Part of the MAD (Massive Atomic Diversity) dataset family. {DESCRIPTION}",  # noqa: E501
        "name": f"{DATASET_NAME}_bench_mptrj",
    },
    "mad-bench-mad-settings.xyz": {
        "id": "DS_dc4lwyrm55p4_0",
        "desc": f"The MAD benchmark dataset, containing a selection of MAD test, MPtrj, Alexandria, SPICE, MD22 and OC2020 datasets, computed with MAD DFT settings. Part of the MAD (Massive Atomic Diversity) dataset family. {DESCRIPTION}",  # noqa: E501
        "name": f"{DATASET_NAME}_bench_mad",
    },
}


def ds_reader(fp):
    split = fp.stem
    pickle_dir = Path(f"pickles/{split}")
    pickle_dir.mkdir(exist_ok=True, parents=True)
    data = iread(fp, format="extxyz", index=":")
    for i, row in enumerate(data):
        row.info["_name"] = f"{DATASET_NAME}_{split}__index_{i}"
        yield AtomicConfiguration.from_ase(row)


def read_wrapper(fp):
    logger.info(f"Reading file: {fp}")
    yield from ds_reader(fp)


for key in DSS:
    fp = DATASET_FP / key
    if not fp.exists():
        logger.warning(f"File {fp} does not exist, skipping.")
        break
    dm = DataManager(
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        configs=read_wrapper(fp),
        prop_map=PROPERTY_MAP,
        dataset_id=DSS[key]["id"],
        standardize_energy=True,
        read_write_batch_size=50000,
    )
    dm.load_co_po_to_vastdb(
        loader=loader,
        batching_ingest=True,
        check_existing=False,
    )
    dm.create_dataset(
        loader=loader,
        name=DSS[key]["name"],
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        other_links=OTHER_LINKS,
        description=DSS[key]["desc"],
        data_license=LICENSE,
        publication_year=PUBLICATION_YEAR,
    )

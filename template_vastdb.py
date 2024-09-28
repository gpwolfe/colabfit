"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
metadata must be in outside layer of keys in PROPERTY_MAP
reader function must return AtomicConfiguration object, not ase.Atoms object
config.info['_name'] must be defined in reader function
all property definitions must be included in DataManager instantiation
set existing doi in create_dataset
set existing dataset_id in create_dataset

add original keys for data to the pi metadata
"""

import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    energy_pd,
    cauchy_stress_pd,
)

load_dotenv()
loader = SparkDataLoader(
    table_prefix="ndb.colabfit.dev",
)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

# Define which tables will be used

loader.config_table = "ndb.colabfit.dev.co_"
loader.prop_object_table = "ndb.colabfit.dev.po_"
loader.config_set_table = "ndb.colabfit.dev.cs_"
loader.dataset_table = "ndb.colabfit.dev.ds_"

# loader.config_table = "ndb.colabfit.dev.co_wip"
# loader.prop_object_table = "ndb.colabfit.dev.po_wip2"
# loader.config_set_table = "ndb.colabfit.dev.cs_wip"
# loader.dataset_table = "ndb.colabfit.dev.ds_wip2"


DATASET_FP = Path("path/to/dataset")
DATASET_NAME = "DATASET_NAME"
AUTHORS = ["author1", "author2"]
PUBLICATION_LINK = "https://example.com"
DATA_LINK = "https://example.com"
OTHER_LINKS = ["https://example.com"]
DESCRIPTION = "Description of dataset"
# DS_LABELS = ["label1", "label2"]
LICENSE = "CC-BY-4.0"
GLOB_STR = "*.extxyz"
DOI = ""
DATASET_ID = ""
PUBLICATION_YEAR = ""

PI_METADATA = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE"},
    "keys": {
        "value": {
            energy_pd["property-name"]: "energy",
            atomic_forces_pd["property-name"]: "forces",
            cauchy_stress_pd["property-name"]: "stress",
            "stress": "stress",
        }
    },
    "input": {"field": "input"},
}
PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        },
    ],
    cauchy_stress_pd["property-name"]: [
        {
            "stress": {"field": "stress", "units": "eV/angstrom^3"},
            "volume-normalized": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}
CO_METADATA = {
    key: {"field": key}
    for key in [
        "constraints",
        "bulk_id",
    ]
}
CONFIGURATION_SETS = [
    ("pattern1", None, "CS_NAME_1", "CS DESCRIPTION 1"),
    ("pattern2", None, "CS_NAME_2", "CS DESCRIPTION 2"),
]


def reader(fp: Path):
    iter_configs = iread(fp, format="extxyz", index=":")
    for i, config in enumerate(iter_configs):
        # config.info["forces"] = forces[i]
        config.info["_name"] = f"{DATASET_NAME}__{fp.stem}__index_{i}"
        yield AtomicConfiguration.from_ase(config, CO_METADATA)


def read_dir(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    data_paths = sorted(list(dir_path.rglob(GLOB_STR)))
    print(data_paths)
    for data_path in data_paths:
        print(f"Reading {data_path}")
        data_reader = reader(data_path)
        for config in data_reader:
            yield config


def main():
    beg = time()
    config_generator = read_dir(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        read_write_batch_size=10000,
        standardize_energy=True,
    )
    print(f"Time to prep: {time() - beg}")
    t = time()
    print("Loading configurations")
    dm.load_co_po_to_vastdb(loader)
    print(f"Time to load: {time() - t}")
    print("Creating configuration sets")
    t = time()
    config_set_rows = dm.create_configuration_sets(
        loader,
        CONFIGURATION_SETS,
    )
    print(f"Time to load: {time() - t}")
    print(config_set_rows)
    print("Creating dataset")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION_LINK,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        # labels=DS_LABELS,
        publication_year=PUBLICATION_YEAR,
    )
    # If running as a script, include below to stop the spark instance
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

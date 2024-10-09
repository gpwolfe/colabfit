"""

Changes to mongodb scripts to convert to vast db scripts

metadata must be in outside layer of keys in PROPERTY_MAP
reader function must return AtomicConfiguration object, not ase.Atoms object
config.info['_name'] must be defined in reader function
all property definitions must be included in DataManager instantiation
set existing doi in create_dataset
set existing dataset_id in create_dataset

add original keys for data to the pi metadata
"""

# Imports


import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd

# Set up data loader environment
load_dotenv()
loader = VastDataLoader(
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

# loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
# loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids_stage4"
# loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
# loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage5"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)
#######################################################################################
# Either set anew or copy over from old script

DATASET_FP = Path("")
DATASET_NAME = ""

# # Set ds_id to same dataset id as before, in order to keep DOI valid

DOI = ""
DATASET_ID = ""

PUBLICATION_YEAR = ""
AUTHORS = [
    "",
    "",
    "",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.1021/acscatal.0c04525"
DATA_LINK = "https://fair-chem.github.io/core/datasets/oc20.html"
DESCRIPTION = ""
LABELS = ["label1", "label2"]


PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-rPBE"},
    "basis_set": {"value": "def2-TZVPP"},
    "input": {
        "value": {
            "EDIFFG": "1E-3",
        },
    },
    "keys": {
        "value": {
            "energy": "free_energy",
            "atomic_forces": "forces",
        },
    },
}

# MAKE SURE METADATA IS MOVED TO OUTSIDE LAYER OF PROPERTY MAP

PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "reference_energy", "units": "eV"},
        }
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        },
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

#######################################################################################
#######################################################################################


def reader(fp):
    for i, config in enumerate(iread(fp, index=":", format="extxyz")):
        config.info["_name"] = f"{fp.stem}__{i}"
        yield AtomicConfiguration.from_ase(config, co_md_map=CO_METADATA)


# In case of directory of files, create wrapper to apply reader function
# Below wrapper allows passing range for files in order to split by slurm job


def wrapper(dir_path: str, range: tuple):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    xyz_paths = sorted(
        list(dir_path.rglob("*.extxyz")),
        key=lambda x: f"{int(x.stem):05d}",
    )
    if range[1] == -1:
        range = (range[0], len(xyz_paths))
    xyz_paths = xyz_paths[range[0] : range[1]]  # noqa E203
    print(f"{range[0]} to {range[1]}")
    for xyz_path in xyz_paths:
        print(f"Reading {xyz_path.name}")
        reader_func = reader(xyz_path)
        for config in reader_func:
            yield config


# def main(range: tuple):
def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = wrapper(DATASET_FP)
    # config_generator = wrapper(DATASET_FP, range)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=100000,
    )
    print(f"Time to prep: {time() - beg}")
    t = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=False)
    print(f"Time to load: {time() - t}")
    print("Creating dataset")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
        doi=DOI,
        labels=LABELS,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    main()

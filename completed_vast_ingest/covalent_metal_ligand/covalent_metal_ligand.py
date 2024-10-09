"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""

import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import energy_pd

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

# loader.config_table = "ndb.colabfit.dev.co_cml"
# loader.prop_object_table = "ndb.colabfit.dev.po_cml"
# loader.config_set_table = "ndb.colabfit.dev.cs_cml"
# loader.dataset_table = "ndb.colabfit.dev.ds_cml"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip"

DATASET_ID = "DS_mnt5vb22pdze_0"
PUBLICATION_YEAR = 2024

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/covalent_metal_ligand"  # noqa
)
DATASET_NAME = (
    "alkali-metal_intercalation_in_disordered_carbon_anode_materials_JMCA2019"
)
AUTHORS = [
    "Jian-Xing Huang",
    "Gábor Csányi",
    "Jin-Bao Zhao",
    "Jun Cheng",
    "Volker L. Deringer",
]
PUBLICATION_LINK = "https://doi.org/10.1039/C9TA05453G"
DATA_LINK = "https://doi.org/10.17863/CAM.42087"
OTHER_LINKS = None
DESCRIPTION = (
    "A dataset created as part of a combination DFT-ML approach to study three "
    "alkali metals (K, Li, Na) in model carbon systems at a range of densities and "
    "degrees of disorder. The purpose of the study was to investigate the properties "
    "of alkali metals in hard (non-graphitising) and nanoporous carbons as potential "
    "anode materials for battery technology."
)
LABELS = None
LICENSE = "CC-BY-4.0"
GLOB_STR = "*.xyz"


PI_METADATA = {
    "software": {"value": "VASP 5.4.4"},
    "method": {"value": "DFT-optB88-vdW"},
    "keys": {
        "value": {
            energy_pd["property-name"]: "energy",
        }
    },
    "input": {
        "value": """
initial forces cutoff = 0.01 eV/Å;
reciprocal space sampled at the gamma point;
electronic smearing of sigma = 0.2 eV;
cut-off energy of 250 eV;
cell shape and volume were kept fixed.
for the optimised structures,
single-point computations;
k-point grid = 3 x 3 x 3;
sigma = 0.1 eV;
cut-off energy = 500 eV."""
    },
}
PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}

CONFIGURATION_SETS = [
    (
        "K-structure",
        None,
        f"{DATASET_NAME}_K_structures",
        f"Configurations from {DATASET_NAME} with that include K atoms",
    ),
    (
        "Li-structure",
        None,
        f"{DATASET_NAME}_Li_structures",
        f"Configurations from {DATASET_NAME} with that include Li atoms",
    ),
    (
        "Na-structure",
        None,
        f"{DATASET_NAME}_Na_structures",
        f"Configurations from {DATASET_NAME} with that include Na atoms",
    ),
]


def reader(fp: Path):
    iter_configs = iread(fp, format="extxyz", index=":")
    for i, config in enumerate(iter_configs):
        config.info["_name"] = f"{DATASET_NAME}__{fp.stem}__config_{i}"
        yield AtomicConfiguration.from_ase(config)


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
        prop_defs=[energy_pd],
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

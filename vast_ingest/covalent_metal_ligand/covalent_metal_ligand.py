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

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_conjugate_pd,
)
from colabfit.tools.utilities import convert_stress
from colabfit.tools.database import generate_ds_id

DATASET_FP = Path("path/to/dataset")
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
DS_DESCRIPTION = ""
DS_LABELS = None
LICENSE = "CC-BY-4.0"
GLOB_STR = "*.xyz"


PI_METADATA = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE"},
    "input": {"field": "input"},
}
PROPERTY_MAP = {
    "energy-conjugate-with-atomic-forces": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        },
    ],
    "cauchy-stress": [
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
    (r".*3.*", None, "MTPU_3_configurations", "MTPU with 3 in the name"),
    (r".*4.*", None, "MTPU_4_configurations", "MTPU with 4 in the name"),
]


def reader(fp: Path):
    iter_configs = iread(fp, format="extxyz")
    for i, config in enumerate(iter_configs):
        # config.info["forces"] = forces[i]
        config.info["_name"] = f"{DATASET_NAME}__file__config_{i}"
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
    load_dotenv()
    loader = SparkDataLoader(table_prefix="ndb.colabfit.dev")
    access_key = os.getenv("SPARK_ID")
    access_secret = os.getenv("SPARK_KEY")
    endpoint = os.getenv("SPARK_ENDPOINT")
    loader.set_vastdb_session(
        endpoint=endpoint, access_key=access_key, access_secret=access_secret
    )
    ds_id = generate_ds_id()
    config_generator = read_dir(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_conjugate_pd, atomic_forces_pd, cauchy_stress_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=ds_id,
        read_write_batch_size=10000,
        standardize_energy=True,
    )
    print("Loading configurations")
    dm.load_co_po_to_vastdb(loader)

    print("Creating configuration sets")
    config_set_rows = dm.create_configuration_sets(
        loader,
        CONFIGURATION_SETS,
    )
    print(config_set_rows)
    print("Creating dataset")
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION_LINK,
        data_link=DATA_LINK,
        description=DS_DESCRIPTION,
        labels=DS_LABELS,
        license=LICENSE,
    )
    # If running as a script, include below to stop the spark instance
    loader.stop_spark()


if __name__ == "__main__":
    main()

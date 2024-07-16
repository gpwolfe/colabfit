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
from functools import partial
from pathlib import Path

from dotenv import load_dotenv

from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_conjugate_pd,
)
from colabfit.tools.parsers import mlip_cfg_reader

DATASET_FP = Path("data/crconi_cao_2022/Training_Cao_20220823.cfg")
DATASET_NAME = "CrCoNi_Cao_2022"
AUTHORS = ["Yifan Cao", "Killian Sheriff", "Rodrigo Freitas"]
PUBLICATION_LINK = "https://arxiv.org/abs/2311.01545"
DATA_LINK = (
    "https://github.com/yifan-henry-cao/MachineLearningPotential"
    "/blob/main/Training_datasets/Training_Cao_20220823.cfg"
)
OTHER_LINKS = ["https://github.com/yifan-henry-cao/MachineLearningPotential"]
DS_DESCRIPTION = (
    "Training dataset that captures chemical short-range order in equiatomic "
    "CrCoNi medium-entropy alloy published with our work Quantifying chemical "
    "short-range order in metallic alloys (description provided by authors)"
)
DS_LABELS = ["equiatomic", "short range order"]
LICENSE = "MIT"
# GLOB_STR = "*.extxyz"


PI_METADATA = {
    "software": {"value": "VASP 6.2.1"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "basis-set": "PAW",
            "k-point-grid": "3x3x3",
            "plane-wave-cutoff": "430 eV",
        }
    },
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
            "stress": {"field": "stress", "units": "kbar"},
            "volume-normalized": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}
# CO_METADATA = {
#     key: {"field": key}
#     for key in [
#         "constraints",
#         "bulk_id",
#     ]
# }
# CONFIGURATION_SETS = [
#     (r".*3.*", None, "MTPU_3_configurations", "MTPU with 3 in the name"),
#     (r".*4.*", None, "MTPU_4_configurations", "MTPU with 4 in the name"),
# ]
element_map = {"0": "Cr", "1": "Co", "2": "Ni"}
reader = partial(mlip_cfg_reader, element_map)


# def reader(fp: Path):
#     iter_configs = iread(fp, format="extxyz")
#     for i, config in enumerate(iter_configs):
#         # config.info["forces"] = forces[i]
#         config.info["_name"] = f"{DATASET_NAME}__file__config_{i}"
#         yield AtomicConfiguration.from_ase(config)


# def read_dir(dir_path: str):
#     dir_path = Path(dir_path)
#     if not dir_path.exists():
#         print(f"Path {dir_path} does not exist")
#         return
#     data_paths = sorted(list(dir_path.rglob(GLOB_STR)))
#     print(data_paths)
#     for data_path in data_paths:
#         print(f"Reading {data_path}")
#         data_reader = reader(data_path)
#         for config in data_reader:
#             yield config


# def main():

load_dotenv()
loader = SparkDataLoader(table_prefix="ndb.colabfit.dev")
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint, access_key=access_key, access_secret=access_secret
)
loader.config_table = "ndb.colabfit.dev.test_co"
loader.prop_object_table = "ndb.colabfit.dev.test_po"
loader.dataset_table = "ndb.colabfit.dev.test_ds"
loader.config_set_table = "ndb.colabfit.dev.test_cs"

config_generator = reader(DATASET_FP)
ds_id = "DS_z2mkok0egrm8_0"
dm = DataManager(
    nprocs=1,
    configs=list(config_generator),
    prop_defs=[energy_conjugate_pd, atomic_forces_pd, cauchy_stress_pd],
    prop_map=PROPERTY_MAP,
    dataset_id=ds_id,
    read_write_batch_size=10000,
    standardize_energy=True,
)
print("Loading configurations")
dm.load_co_po_to_vastdb(loader)

# print("Creating configuration sets")
# config_set_rows = dm.create_configuration_sets(
#     loader,
#     CONFIGURATION_SETS,
# )
# print(config_set_rows)
print("Creating dataset")
dm.create_dataset(
    loader,
    name=DATASET_NAME,
    authors=AUTHORS,
    publication_link=PUBLICATION_LINK,
    data_link=DATA_LINK,
    description=DS_DESCRIPTION,
    labels=DS_LABELS,
)
# If running as a script, include below to stop the spark instance
#     loader.stop_spark()


# if __name__ == "__main__":
#     main()

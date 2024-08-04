"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
there appear to be some duplicates of vasprun files, prepended with bp_
these should perhaps not be included in the dataset
in addition, some files appear to represent failed trajectories
and are not parsable, having no relevant data. 

"""

import os
from pathlib import Path
from collections import Counter

from ase.io import read
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_conjugate_pd,
)
from colabfit.tools.database import generate_ds_id

DATASET_FP = Path("data/23-Single-Element-DNPs")
# sorted(list(Path("data/23-Single-Element-DNPs").rglob("*vasprun.xml")))
DATASET_NAME = "23_Single_Element_DNPs_all_trajectories"
AUTHORS = ["Christopher M. Andolina", "Wissam A. Saidi"]
PUBLICATION_LINK = "https://doi.org/10.1039/D3DD00046J"
DATA_LINK = "https://github.com/saidigroup/23-Single-Element-DNPs"
OTHER_LINKS = None
DS_DESCRIPTION = (
    "The full trajectories from the VASP runs used to "
    "generate the 23-Single-Element-DNPs training sets."
)
DS_LABELS = None
LICENSE = "GPL-3.0"
GLOB_STR = "*vasprun.xml"


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


CONLEN = []
images = []


def reader(fp):
    try:
        allatoms = read(fp, index=":", format="vasp-xml")
        CONLEN.append(len(allatoms))
        for atoms in allatoms:
            atoms.info["energy"] = atoms.get_potential_energy()
            atoms.arrays["forces"] = atoms.get_forces()
            atoms.info["stress"] = atoms.get_stress(voigt=False)
            atoms.info["forces"] = atoms.get_forces()
            atoms.info["_name"] = str(fp).replace(str(DATASET_FP), "")
            atoms.info["labels"] = []
            for part in atoms.info["_name"].split("/"):
                if part == "":
                    continue
                elif part[0] == "T":
                    atoms.info["labels"].append(part)
                elif part.split("_")[0] in ["vacancies", "interstitials", "elastic"]:
                    atoms.info["labels"].append(part)
                elif "_mp-" in part:
                    atoms.info["labels"].append(part)
            if atoms.info["labels"] == []:
                atoms.info["labels"] = None
            config = AtomicConfiguration.from_ase(atoms)
            yield config
    except Exception as e:
        CONLEN.append(0)
        print(f"Error reading {fp}: {e}")
        return


def read_dir(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    data_paths = sorted(list(dir_path.rglob(GLOB_STR)))
    # print(data_paths)
    for data_path in data_paths:
        # print(f"Reading {data_path}")
        data_reader = reader(data_path)
        for config in data_reader:
            yield config


# def main():
load_dotenv()
loader = SparkDataLoader(table_prefix="ndb.colabfit.dev")
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint, access_key=access_key, access_secret=access_secret
)
loader.config_table = "ndb.colabfit.dev.cos_from_ingest"
loader.prop_object_table = "ndb.colabfit.dev.pos_from_ingest"
loader.config_set_table = "ndb.colabfit.dev.23_dnps_cs"
loader.dataset_table = "ndb.colabfit.dev.ds_from_ingest"

# ds_id = generate_ds_id()
ds_id = "DS_rw483e9cedsy_0"
config_generator = read_dir(DATASET_FP)
dm = DataManager(
    nprocs=1,
    configs=config_generator,
    prop_defs=[energy_conjugate_pd, atomic_forces_pd, cauchy_stress_pd],
    prop_map=PROPERTY_MAP,
    dataset_id=ds_id,
    read_write_batch_size=5000,
    standardize_energy=True,
)
print("Loading configurations")
dm.load_co_po_to_vastdb(loader)
c = Counter(CONLEN)
print(c)
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
    data_license=LICENSE,
)
# If running as a script, include below to stop the spark instance
loader.stop_spark()


# if __name__ == "__main__":
#     main()

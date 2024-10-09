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
from time import time

import numpy as np
from ase.io import read
from dotenv import load_dotenv
from tqdm import tqdm

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)

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

# loader.config_table = "ndb.colabfit.dev.co_23l"
# loader.prop_object_table = "ndb.colabfit.dev.po_23l"
# loader.config_set_table = "ndb.colabfit.dev.cs_23l"
# loader.dataset_table = "ndb.colabfit.dev.ds_23l"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip"


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/23-Single-Element-DNPs"  # noqa E501
)
DATASET_NAME = "23-Single-Element-DNPs_all_trajectories"
AUTHORS = ["Christopher M. Andolina", "Wissam A. Saidi"]
PUBLICATION_LINK = "https://doi.org/10.1039/D3DD00046J"
DATA_LINK = "https://github.com/saidigroup/23-Single-Element-DNPs"
OTHER_LINKS = None
DESCRIPTION = (
    "The full trajectories from the VASP runs used to "
    "generate the 23-Single-Element-DNPs training sets. Configuration sets are "
    "available for each element."
)
DS_LABELS = None
LICENSE = "GPL-3.0"
GLOB_STR = "*vasprun.xml"

DATASET_ID = "DS_whwcq2rzt2of_0"
DOI = None
PUBLICATION_YEAR = "2024"

PI_METADATA = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": """# Saidi Group University of Pittsburgh INCAR file for generating VASP training data at variable temperatures 2020.
PREC=A
ENCUT=400
ISYM=0
ALGO=fast
EDIFF=1E-8
LREAL=F
NPAR=8
KPAR=16
NELM=200
NELMIN=4
ISTART=0
ICHARG=0
ISIF=2
ISMEAR=1
SIGMA=0.15
MAXMIX=50
IBRION=0
NBLOCK=1
KBLOCK=10
SMASS=-1
POTIM=2
TEBEG=XXX  # Set initial temperature for NVT
TEEND=XXX  # Set final temperature for NVT
NSW=20     # Adjust to change the number of structures
#opt
# ISIF =0 ! relax ions only do not calculated stress tensor
# ISIF =3 ! relax ions and vol
# potim = 0.1 ! relax ions and volume as needed
# NSW = 1000
# IBRION = 2 !use CG
LWAVE=F
LCHARG=T
PSTRESS=0
KSPACING=0.24
KGAMMA=F
#ispin=2
"""
    },
    "property_keys": {
        "value": {
            energy_pd["property-name"]: "e_0_energy",
            atomic_forces_pd["property-name"]: "forces",
            cauchy_stress_pd["property-name"]: "stress",
        }
    },
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
            "stress": {
                "field": "stress",
                "units": "eV/angstrom^3",
            },  # using ASE converts the VASP xml units of kbar (.1 GPa) to eV/angstrom^3 # noqa
            "volume-normalized": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}

CONFIGURATION_SETS = [
    (
        f"{elem}__",
        None,
        f"{DATASET_NAME}_{elem}",
        f"Trajectories of element {elem} from {DATASET_NAME}",
    )
    for elem in [
        "Ag",
        "Al",
        "Au",
        "Co",
        "Cu",
        "Ge",
        "I",
        "Kr",
        "Li",
        "Mg",
        "Mo",
        "Nb",
        "Ni",
        "Os",
        "Pb",
        "Pd",
        "Pt",
        "Re",
        "Sb",
        "Sr",
        "Ti",
        "Zn",
        "Zr",
    ]
]


def reader(fp):
    try:
        configs = read(fp, index=":", format="vasp-xml")
    except Exception as e:
        print(f"Error reading {fp}: {e}")
        return
    for i, atoms in enumerate(configs):
        try:
            atoms.info["energy"] = atoms.get_potential_energy()
            atoms.arrays["forces"] = atoms.get_forces()
            stress = atoms.get_stress(voigt=False)
            if not (np.isnan(stress)).any():
                atoms.info["stress"] = stress
            atoms.info["_name"] = (
                "__".join(
                    [
                        x
                        for x in Path(str(fp).replace(str(DATASET_FP), "")).parts
                        if x != "/"
                    ]
                )
                + f"__index_{i}"
            )
            labels = []
            for part in atoms.info["_name"].split("__"):
                if part == "":
                    continue
                elif part[0] == "T" and part[1:].isnumeric():
                    labels.append(f"temperature:{part[1:]}")
                elif part.isnumeric() and len(part) > 2:
                    labels.append(f"temperature:{part}")
                elif part.split("_")[0] in ["vacancies", "interstitials", "elastic"]:
                    labels.append(part)
                elif "_mp-" in part:
                    labels.append(f"materials_project_id:{part.split('_')[-1]}")
            if labels == []:
                atoms.info["_labels"] = None
            else:
                atoms.info["_labels"] = labels
            config = AtomicConfiguration.from_ase(atoms)
            yield config
        except Exception as e:
            print(f"Error reading {str(fp).replace(str(DATASET_FP), '')}: {e}")
            continue


def read_dir(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    data_paths = sorted(list(dir_path.rglob(GLOB_STR)))
    for data_path in tqdm(data_paths):
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
        read_write_batch_size=100000,
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

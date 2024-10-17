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

add original keys for data to the pi metadata


The configurations can't effectively be added to config sets with our schema. The
configs are exactly the same, only the properties differ, so all configs have a
bunch of POs pointing to them, and there's no way to specify which POs belong in
which CS.

Rename ch2o_3601-RKHS.npz to ch2o_3601-RKHS-clean.npz to allow select on file name

"""

import os
from collections import defaultdict
from pathlib import Path
from time import time

import numpy as np
from ase.atoms import Atoms
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
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
loader.metadata_dir = "test_md/MD"  # comment out upon full ingest

# loader.config_table = "ndb.colabfit.dev.co_aee"
# loader.prop_object_table = "ndb.colabfit.dev.po_aee"
# loader.config_set_table = "ndb.colabfit.dev.cs_aee"
# loader.dataset_table = "ndb.colabfit.dev.ds_aee"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_aee"

loader.config_table = "ndb.colabfit.dev.co_wip"
# loader.prop_object_table = "ndb.colabfit.dev.po_wip"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
# loader.dataset_table = "ndb.colabfit.dev.ds_wip"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_wip"

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/"
    "gw_scripts/data/aleatoric_epistemic_error/"
)
DATASET_NAME = "aleatoric_epistemic_error_AIC2023"
AUTHORS = ["Sugata Goswami", "Silvan Käser", "Raymond J. Bemish", "Markus Meuwly"]
PUBLICATION_LINK = "https://doi.org/10.1016/j.aichem.2023.100033"
DATA_LINK = "https://github.com/MMunibas/noise"
OTHER_LINKS = None
DESCRIPTION = (
    "Dataset for H2CO, with and without added noise for testing "
    "the effects of noise on quality of fit. Configurations sets are included "
    "for clean energy values with different levels of gaussian noise added to "
    "atomic forces (including a set with no noise added), and energies perturbed "
    "at different levels (including a set with no perturbation). Configuration sets "
    "correspond to individual files found at the data link."
)
# DS_LABELS = ["label1", "label2"]
LICENSE = "CC-BY-NC-ND-4.0"
DOI = None
DATASET_ID = "DS_pms72aqfzc4v_0"
PUBLICATION_YEAR = "2024"

PI_METADATA = {
    "software": {"value": "Gaussian 09"},
    "method": {"value": "MP2"},
    "keys": {
        "value": {
            energy_pd["property-name"]: "E",
            atomic_forces_pd["property-name"]: "F",
        }
    },
    "charge": {"field": "charge"},
    "dipole_moment": {"field": "dipole_moment"},
    "input": {
        "value": {
            "basis-set": "aug-cc-pVTZ",
        },
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
    "_metadata": PI_METADATA,
}

CO_MD = {
    "energy-perturbation": {"field": "energy-perturbation"},
    "force-gaussian-noise": {"field": "force-gaussian-noise"},
}

file_map = {
    "ch2o_3601-RKHS-gaussian-noise-force-SD-10-6.npz": (
        f"{DATASET_NAME}__ch2o_noise_force_10-6",  # noqa E501
        "Configurations with clean RKHS energies and noisy forces with Gaussian noise with amplitude of 10-6 eV/angstrom (2.31 X 10-5 kcal/mol/angstrom)",  # noqa E501
        "0",
        "10-6 eV/angstrom",
    ),
    "ch2o_3601-RKHS-clean.npz": (
        f"{DATASET_NAME}__ch2o_3601-RKHS-clean",
        "Configurations containing clean RKHS energies. Forces are zero.",
        "0",
        "not applicable",
    ),
    "ch2o_3601-RKHS-gaussian-noise-SD-0-0000001.npz": (
        f"{DATASET_NAME}__ch2o_perturbed_energy_10-7",  # noqa E501
        "Configurations containing perturbed energies with amplitude of 10-7 eV (2.31 X 10-6 kcal/mol)",  # noqa E501
        "10-7 eV",
        "0",
    ),
    "ch2o_3601-RKHS-gaussian-noise-force-SD-10-7.npz": (
        f"{DATASET_NAME}__ch2o_noise_force_10-7",  # noqa E501
        "Configurations with clean RKHS energies and noisy forces with Gaussian noise with amplitude of 10-7 eV/angstrom (2.31 X 10-6 kcal/mol/angstrom)",  # noqa E501
        "0",
        "10-7 eV/angstrom",
    ),
    "ch2o_3601-RKHS-force-energy.npz": (
        f"{DATASET_NAME}__ch2o_clean-force-energy",
        "Configurations with clean RKHS energies and forces",
        "0",
        "0",
    ),
    "ch2o_3601-RKHS-gaussian-noise-force-SD-10-5.npz": (
        f"{DATASET_NAME}_ch2o_noise_force_10-5",  # noqa E501
        "Configurations with clean RKHS energies and noisy forces with Gaussian noise with amplitude of 10-5 eV/angstrom (2.31 X 10-4 kcal/mol/angstrom)",  # noqa E501
        "0",
        "10-5 eV/angstrom",
    ),
    "ch2o_3601-RKHS-gaussian-noise-SD-0-000001.npz": (
        f"{DATASET_NAME}_ch2o_perturbed_energy_10-6",  # noqa E501
        "Configurations containing perturbed energies with amplitude of 10-6 eV (2.31 X 10-5 kcal/mol)",  # noqa E501
        "10-6 eV",
        "0",
    ),
    "ch2o_3601-RKHS-gaussian-noise-SD-0-00001.npz": (
        f"{DATASET_NAME}_ch2o_perturbed_energy_10-5",  # noqa E501
        "Configurations containing perturbed energies with amplitude of 10-5 eV (2.31 X 10-4 kcal/mol)",  # noqa E501
        "10-5 eV",
        "0",
    ),
}
CONFIGURATION_SETS = [
    (key.replace(".npz", ""), None, value[0], value[1])
    for key, value in file_map.items()
]


def reader(filepath):
    data = defaultdict(list)
    energy_perturbation = file_map[filepath.name][2]
    force_gaussian_noise = file_map[filepath.name][3]
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
    # Keys = ['E', 'D', 'F', 'N', 'Q', 'R', 'Z']
    # E = energies
    # D = dipole moment
    # F = forces
    # N = number of atoms
    # Q = charge
    # R = positions
    # Z = atomic numbers
    for i, coords in enumerate(data["R"]):
        atoms = Atoms(
            numbers=data["Z"][i],
            positions=coords,
        )
        atoms.info.update(
            {
                "energy-perturbation": energy_perturbation,
                "force-gaussian-noise": force_gaussian_noise,
                "energy": data["E"][i],
                "forces": data["F"][i],
                "dipole_moment": data["D"][i],
                "charge": data["Q"][i],
                "_name": f"{filepath.stem}__index_{i}",
                "_labels": [],
            }
        )
        yield AtomicConfiguration.from_ase(atoms, CO_MD)


def read_dir(dir_path: Path):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    data_paths = sorted(list(dir_path.rglob("*.npz")))
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
        prop_defs=[energy_pd, atomic_forces_pd],
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
    # loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
same h5 file setup as ANI2x
"""

import os
from pathlib import Path
from time import time

import h5py
import numpy as np
from ase.atoms import Atoms
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    # atomic_forces_pd,
    energy_pd,
)

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


# loader.config_table = "ndb.colabfit.dev.co_comp"
# loader.config_set_table = "ndb.colabfit.dev.cs_comp"
# loader.dataset_table = "ndb.colabfit.dev.ds_comp"
# loader.prop_object_table = "ndb.colabfit.dev.po_comp"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/COMP6v2-wB97MD3BJ-def2TZVPP.h5"  # noqa
)
DATASET_NAME = "COMP6v2-wB97MD3BJ-def2TZVPP"
LICENSE = "CC-BY-4.0"  # Creative Commons Attribution 4.0 International
PUBLICATION_YEAR = "2024"
DOI = None
DATASET_ID = "DS_mznjdz4oqv11_0"
PUBLICATION = "https://doi.org/10.1021/acs.jctc.0c00121"
DATA_LINK = "https://doi.org/10.5281/zenodo.10126157"
# OTHER_LINKS = []

AUTHORS = [
    "Kate Huddleston",
    "Roman Zubatyuk",
    "Justin Smith",
    "Adrian Roitberg",
    "Olexandr Isayev",
    "Ignacio Pickering",
    "Christian Devereux",
    "Kipton Barros",
]

DESCRIPTION = (
    "COMP6v2-wB97MD3BJ-def2TZVPP is the portion of COMP6v2 calculated at the "
    "wB97MD3BJ/def2TZVPP level of theory. "
    "COmprehensive Machine-learning Potential (COMP6) Benchmark Suite "
    "version 2.0 is an extension of the COMP6 benchmark found in "
    "the following repository: https://github.com/isayev/COMP6. COMP6v2 is a data set "
    "of density functional properties for molecules containing H, C, N, O, S, F, and "
    "Cl. It is available at the following levels of theory: wB97X/631Gd (data used to "
    "train model in the ANI-2x paper); wB97MD3BJ/def2TZVPP; wB97MV/def2TZVPP; "
    "B973c/def2mTZVP. The 6 subsets from COMP6 (ANI-MD, DrugBank, GDB07to09, GDB10to13 "
    "Tripeptides, and s66x8) are contained in each of the COMP6v2 datasets "
    "corresponding to the above levels of theory."
)

PI_METADATA = {
    "software": {"value": "ORCA 4.2.1"},
    "method": {"value": "DFT-wB97MV"},
    "keys": {"value": {energy_pd["property-name"]: "energies"}},
    "basis-set": {"value": "def2TZVPP"},
    "dipole": {"field": "dipole", "units": "electron angstrom"},
    "wB97M-def2-TZVPP-scf-energy": {"field": "scf_energy", "units": "hartree"},
    "D3-energy-corrections": {"field": "en_correction", "units": "hartree"},
    "input": {
        "value": """Step 1:
!  quick-dft slowconv loosescf
%scf maxiter 256 end
Step 2:
!  wB97m-d3bj def2-tzvpp def2/j rijcosx engrad tightscf SCFConvForced soscf grid4 \
finalgrid6 gridx7
%elprop dipole true quadrupole true end
%output PrintLevel mini Print[P
DFTD
GRAD] 1 end
%scf maxiter 256 end
!  MORead
%moinp "PATH TO .gbw FILE FROM STEP 1"
Step 3:
!  wb97m-v def2-tzvpp def2/j rijcosx tightscf ScfConvForced grid4 finalgrid6 gridx7 \
vdwgrid4
!  MORead
%moinp \"PATH TO .gbw FILE FROM STEP 2\""""
    },
}


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}


def compv6_reader(properties, num_atoms):
    coordinates = properties["coordinates"]
    species = properties["species"]
    energies = properties["energies"]
    en_correction = properties["D3.energy-corrections"]
    scf_energies = properties["wB97M_def2-TZVPP.scf-energies"]
    dipoles = properties["dipoles"]
    for i, coord in enumerate(coordinates):
        config = Atoms(
            positions=coord,
            numbers=species[i],
        )
        config.info["energy"] = energies[i]
        config.info["en_correction"] = en_correction[i]
        config.info["scf_energy"] = scf_energies[i]
        config.info["dipole"] = dipoles[i]
        config.info["_name"] = (
            f"COMP6v2-wB97MD3BJ-def2TZVPP__natoms_{num_atoms}__ix_{i}"
        )
        for k, v in config.info.items():
            if isinstance(v, np.ndarray):
                config.info[k] = v.tolist()
        yield AtomicConfiguration.from_ase(config)


def wrapper(fp):
    with h5py.File(fp) as h5:
        for key in h5.keys():
            try:
                yield from compv6_reader(h5[key], key)
            except Exception as e:
                print(e)
                # handle end of group
                pass


# def main(range: tuple):
def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = wrapper(DATASET_FP)
    # config_generator = wrapper(DATASET_FP, range)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd],
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
        # labels=LABELS,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys
    # range = (int(sys.argv[1]), int(sys.argv[2]))
    main()

"""
author: Gregory Wolfe, Alexander Tao

File notes
----------
Lattice
Properties=species:S:1:pos:R:3:magnetic_moments:R:1:forces:R:3
virial
energy
stress
free_energy
"energy_our GAP"=-22.927362757204524
"binding_energy_our GAP"=-2.2510694511666527
"energy_GAP Dragoni"=-13838.48512957841
"binding_energy_GAP Dragoni"=1.1654634043225087
"energy_EAM Mendelev"=-7.652258547705728
"binding_energy_EAM Mendelev"=-1.913064636926432
energy_Finnis-Sinclair=-6.819798926783503
binding_energy_Finnis-Sinclair=-1.7049497316958757
pbc="T T T"


"""

import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    energy_pd,
    cauchy_stress_pd,
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


# loader.config_table = "ndb.colabfit.dev.co_fenano"
# loader.config_set_table = "ndb.colabfit.dev.cs_fenano"
# loader.dataset_table = "ndb.colabfit.dev.ds_fenano"
# loader.prop_object_table = "ndb.colabfit.dev.po_fenano"

loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage2"
loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

# DS_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/Iron_nanoparticle/")
DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data2/"
    "data/fe_nanoparticles_prb_2023/convex_hull.xyz"
)  # local

DATASET_NAME = "Fe_nanoparticles_PRB_2023"
AUTHORS = ["Richard Jana", "Miguel A. Caro"]
PUBLICATION_YEAR = "2023"
LICENSE = "CC-BY-4.0"
DATASET_ID = "DS_5h3810yhu4wj_0"
DOI = "10.60732/20ba88af"
PUBLICATION = "https://doi.org/10.1103/PhysRevB.107.245421"
DATA_LINK = "https://doi.org/10.5281/zenodo.7632315"
# LINKS = [
#     "https://doi.org/10.1103/PhysRevB.107.245421",
#     "https://doi.org/10.5281/zenodo.7632315",
# ]
DESCRIPTION = (
    "This iron nanoparticles database contains dimers; trimers; bcc, fcc, "
    "hexagonal close-packed (hcp), simple cubic, and diamond crystalline structures. "
    "A wide range of cell parameters, as well as rattled structures, bcc-fcc and "
    "bcc-hcp transitional structures, surface slabs cleaved from relaxed bulk "
    "structures, nanoparticles and liquid configurations are included. "
    "The energy, forces and virials for the atomic structures were computed at the "
    "DFT level of theory using VASP with the PBE functional and standard PAW "
    "pseudopotentials for Fe (with 8 valence electrons, 4s^23d^6). The kinetic "
    "energy cutoff for plane waves was set to 400 eV and the energy threshold for "
    "convergence was 10-7 eV. All the DFT calculations "
    "were carried out with spin polarization."
)

PI_MD = {
    "method": {"value": "DFT-PBE"},
    "software": {"value": "VASP"},
    "input": {
        "value": {
            "encut": {"value": 400, "units": "eV"},
            "ediff": 10e-7,
            "ispin": 2,
        }
    },
}

PI_MD.update(
    {
        key: {"field": key}
        for key in [
            "magnetic_moments",
            "energy_our_GAP",
            "binding_energy_our_GAP",
            "energy_GAP_Dragoni",
            "binding_energy_GAP_Dragoni",
            "energy_EAM_Mendelev",
            "binding_energy_EAM_Mendelev",
            "energy_Finnis-Sinclair",
            "binding_energy_Finnis-Sinclair",
        ]
    }
)

PROPERTY_MAP = {
    # "potential-energy": [
    #     {
    #         "energy": {"field": "energy", "units": "eV"},
    #         "per-atom": {"value": False, "units": None},
    #     }
    # ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        }
    ],
    energy_pd["property-name"]: [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    cauchy_stress_pd["property-name"]: [
        {
            "stress": {"field": "stress", "units": "kbar"},
            "volume-normalized": {"value": False, "units": None},
        },
        # {
        #     "stress": {"field": "virial", "units": "kbar"},
        #     "volume-normalized": {"value": True, "units": None},
        # },
    ],
    "_metadata": PI_MD,
}


def reader(fp):
    data = iread(fp, index=":", format="extxyz")
    for i, config in enumerate(data):
        config.info["magnetic_moments"] = config.arrays["magnetic_moments"]
        config.info["_name"] = f"{fp.stem}__{i}"
        info = config.info.copy()
        for key, val in info.items():
            if " " in key:
                key = key.replace(" ", "_")
                config.info[key] = val
        yield AtomicConfiguration.from_ase(config)


# def wrapper(dir_path: str):
#     dir_path = Path(dir_path)
#     if not dir_path.exists():
#         print(f"Path {dir_path} does not exist")
#         return
#     xyz_paths = sorted(
#         list(dir_path.rglob(GLOB_STR)),
#         key=lambda x: x.stem,
#     )
#     for xyz_path in xyz_paths:
#         print(f"Reading {xyz_path.name}")
#         reader_func = reader(xyz_path)
#         for config in reader_func:
#             yield config


def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=100000,
    )
    print(f"Time to prep: {time() - beg}")
    # t = time()
    # dm.load_co_po_to_vastdb(loader, batching_ingest=False)
    # print(f"Time to load: {time() - t}")
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
        # labels=labels,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

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

from dotenv import load_dotenv

from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_conjugate_pd,
)
from colabfit.tools.database import generate_ds_id
from colabfit.tools.parsers import vasp_outcar_wrapper


DATASET_FP = Path("data/lin_mag_cr2o3/archive/VASP_and_Elk")
DATASET_NAME = "on_the_sign_of_the_linear_magnetoelectric_coefficient_in_Cr2O3"
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "http://doi.org/10.48550/arXiv.2309.02095"
DATA_LINK = "https://doi.org/10.24435/materialscloud:ek-fp"
# OTHER_LINKS = []

AUTHORS = [
    "Eric Bousquet",
    "Eddy Lelièvre-Berna",
    "Navid Qureshi",
    "Jian-Rui Soh",
    "Nicola Ann Spaldin",
    "Andrea Urru",
    "Xanthe Henderike Verbeek",
    "Sophie Francis Weber",
]
DATASET_DESC = ""
ELEMENTS = None
GLOB_STR = "OUTCAR*"

PI_METADATA = {
    "software": {"value": "VASP     "},
    "method": {"value": ""},
    "input": {"field": "input"},
}

PROPERTY_MAP = {
    energy_conjugate_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "eV/angstrom^3"},
    #         "volume-normalized": {"value": False, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

# CO_METADATA = {
#     "enthalpy": {"field": "h", "units": "Ha"},
#     "zpve": {"field": "zpve", "units": "Ha"},
# }
CONFIGURATION_SETS = [
    (
        r".*Induced_magnetic_moments.*",
        None,
        f"{DATASET_NAME}_Induced_magnetic_moments",
        f"{DATASET_NAME} configurations from induced magnetic moment calculations",
    ),
    (
        r".*Structural_relaxation.*",
        None,
        f"{DATASET_NAME}_Structural_relaxation",
        f"{DATASET_NAME} configurations from structural relaxation calculations",
    ),
    (
        r".*Born_effective_charges.*",
        None,
        f"{DATASET_NAME}_displacement_Born_effective_charges",
        f"{DATASET_NAME} configurations with displaced atoms from Born effective "
        "charges calculations",
    ),
]

ds_id = generate_ds_id()
print(f"Dataset ID: {ds_id}\nDS Name: {DATASET_NAME}")

load_dotenv()
loader = SparkDataLoader(table_prefix="ndb.colabfit.dev")
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint, access_key=access_key, access_secret=access_secret
)

config_generator = vasp_outcar_wrapper(DATASET_FP)
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
    publication_link=PUBLICATION,
    data_link=DATA_LINK,
    description=DATASET_DESC,
    labels=DS_LABELS,
    dataset_id=ds_id,
)
# If running as a script, include below to stop the spark instance
loader.stop_spark()

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

from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_conjugate_pd,
)
from colabfit.tools.database import generate_ds_id
from colabfit.tools.parsers import vasp_outcar_wrapper

DATASET_FP = Path("path/to/dataset_dir")
DATASET_NAME = "DATASET_NAME"
AUTHORS = ["author1", "author2"]
PUBLICATION_LINK = "https://example.com"
DATA_LINK = "https://example.com"
OTHER_LINKS = ["https://example.com"]
DS_DESCRIPTION = "Description of dataset"
DS_LABELS = ["label1", "label2"]
LICENSE = "CC-BY-4.0"


DATASET_FP = Path("")
DATASET_NAME = ""
LICENSE = "CC-BY-4.0"
PUB_YEAR = ""

PUBLICATION = ""
DATA_LINK = ""
# OTHER_LINKS = []

AUTHORS = [""]
DATASET_DESC = ""
ELEMENTS = None
GLOB_STR = "OUTCAR"

PI_METADATA = {
    "software": {"value": ""},
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
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "kilobar"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "enthalpy": {"field": "h", "units": "Ha"},
    "zpve": {"field": "zpve", "units": "Ha"},
}

# Name-regex, label-regex, configuration-set name, configuration-set description
CONFIGURATION_SETS = [
    (r".*3.*", None, "MTPU_3_configurations", "MTPU with 3 in the name"),
    (r".*4.*", None, "MTPU_4_configurations", "MTPU with 4 in the name"),
]


ds_id = generate_ds_id()
print(f"Dataset ID: {ds_id}\nDS Name: {DATASET_NAME}")

load_dotenv()
loader = VastDataLoader(table_prefix="ndb.colabfit.dev")
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
    publication_link=PUBLICATION_LINK,
    data_link=DATA_LINK,
    description=DS_DESCRIPTION,
    labels=DS_LABELS,
    dataset_id=ds_id,
)
# If running as a script, include below to stop the spark instance
loader.stop_spark()

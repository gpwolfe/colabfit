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
set existing doi in create_dataset
set existing dataset_id in create_dataset

add original keys for data to the pi metadata
"""

import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv
from tqdm import tqdm

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/carbon_allotrope/dataset"
)
DATASET_NAME = "Carbon_allotrope_multilayer_graphene_graphite_PRB2019"
AUTHORS = ["Mingjian Wen", "Ellad B. Tadmor"]
PUBLICATION_LINK = "https://doi.org/10.1103/PhysRevB.100.195419"
DATA_LINK = (
    "https://journals.aps.org/prb/supplemental/10.1103/PhysRevB.100.195419/dataset.tar"
)
# OTHER_LINKS = ["https://example.com"]
DESCRIPTION = """
The dataset consists of energies and forces for pristine and defected monolayer graphene, bilayer graphene, and
graphite in various states. The configurations in the dataset are generated in two ways: (1) crystals with distortions
(compression and stretching of the simulation cell together with random perturbations of atoms), and (2) configura-
tions drawn from ab initio molecular dynamics (AIMD) trajectories at 300, 900, and 1500 K.
For monolayer graphene, the configurations include:
* pristine
- In-plane compressed and stretched monolayers
- AIMD trajectories
* defected
- Configurations from the minimization of a monolayer with a single vacancy
- AIMD trajectories of monolayers with a single vacancy
For bilayer graphene, the configurations include:
* pristine
- AB-stacked bilayers with compression and stretching in the basal plane
- Bilayers with different translational registry (e.g. AA, AB, and SP) at various layer separations
- Twisted bilayers with different twisting angles at various layer separations
- AIMD trajectories of twisted bilayers and bilayers in AB and AA stackings
* defected
- Configurations from the minimization of a bilayer with a single vacancy in each layer
- AIMD trajectories of a bilayer with a single vacancy in one layer and the other layer pristine
- AIMD trajectories of a bilayer with a single vacancy in each layer; Initial configuration without interlayer
bonds
- AIMD trajectories of a bilayer with a single vacancy in each layer; Initial configuration with interlayer
bonds formed
For graphite, the configurations include:
* pristine
- Graphite with compression and stretching in the basal plane
- Graphite with compression and stretching along the c-axis
- AIMD trajectories
"""

# DS_LABELS = ["label1", "label2"]
LICENSE = "CC-BY-4.0"
GLOB_STR = "*.xyz"
DOI = None
DATASET_ID = "DS_zn8tf2kmgrdm_0"
PUBLICATION_YEAR = "2024"

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

# loader.config_table = "ndb.colabfit.dev.co_callo"
# loader.prop_object_table = "ndb.colabfit.dev.po_callo"
# loader.config_set_table = "ndb.colabfit.dev.cs_callo"
# loader.dataset_table = "ndb.colabfit.dev.ds_callo"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip2"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip2"

PI_METADATA = {
    "software": {"value": "VASP 5.x.x"},
    "method": {"value": "DFT-PBE"},
    "keys": {
        "value": {
            energy_pd["property-name"]: "Energy",
            atomic_forces_pd["property-name"]: "force",
        }
    },
    "input": {
        "value": {
            "van_der_waals_correction": "MBD",
            "ecut": "500 eV",
        }
    },
}
PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "Energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "force", "units": "eV/angstrom"},
        },
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
CONFIGURATION_SETS = [
    (
        "bilayer__",
        None,
        f"{DATASET_NAME}__bilayer_graphene",
        f"Configurations from {DATASET_NAME} of bilayer graphene",
    ),
    (
        "graphite__",
        None,
        f"{DATASET_NAME}__graphite",
        f"Configurations from {DATASET_NAME} of graphite",
    ),
    (
        "monolayer__",
        None,
        f"{DATASET_NAME}__monolayer_graphene",
        f"Configurations from {DATASET_NAME} of monolayer graphene",
    ),
]


def namer(fp: Path):
    name = str(fp).replace(str(DATASET_FP), "").split("/")
    return "__".join([x for x in name if x != ""])


def reader(fp: Path):
    iter_configs = iread(fp, format="extxyz", index=":")
    for i, config in enumerate(iter_configs):
        # config.info["forces"] = forces[i]
        name = namer(fp)
        config.info["_name"] = f"{name}__index_{i}"
        yield AtomicConfiguration.from_ase(config)


def read_dir(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    data_paths = sorted(list(dir_path.rglob(GLOB_STR)))
    print(data_paths)
    for data_path in tqdm(data_paths):
        # print(f"Reading {data_path.stem}")
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
    dm.create_configuration_sets(
        loader,
        CONFIGURATION_SETS,
    )
    print(f"Time to load: {time() - t}")
    print("Creating dataset")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION_LINK,
        data_link=DATA_LINK,
        # other_links=OTHER_LINKS,
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

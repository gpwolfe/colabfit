"""
author: Gregory Wolfe

Properties
----------
energy
virial
forces

Other properties added to metadata
----------------------------------

File notes
----------

xyz file header
lattice
energy
virial
Properties=species:S:1:pos:R:3:force:R:3

"""

import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    SparkDataLoader,
)
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)

# Set up data loader environment
load_dotenv()
loader = SparkDataLoader(
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


# loader.config_table = "ndb.colabfit.dev.co_unep"
# loader.prop_object_table = "ndb.colabfit.dev.po_unep"
# loader.config_set_table = "ndb.colabfit.dev.cs_unep"
# loader.dataset_table = "ndb.colabfit.dev.ds_unep"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip2"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip2"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/unep"
)
DATASET_NAME = "UNEP-v1_2023"


PUBLICATION_YEAR = "2023"
LICENSE = "CC-BY-4.0"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.48550/arXiv.2311.04732"
DATA_LINK = "https://zenodo.org/doi/10.5281/zenodo.10081676"
LINKS = [
    "https://zenodo.org/doi/10.5281/zenodo.10081676",
    "https://doi.org/10.48550/arXiv.2311.04732",
]
AUTHORS = [
    "Keke Song",
    "Rui Zhao",
    "Jiahui Liu",
    "Yanzhou Wang",
    "Eric Lindgren",
    "Yong Wang",
    "Shunda Chen",
    "Ke Xu",
    "Ting Liang",
    "Penghua Ying",
    "Nan Xu",
    "Zhiqiang Zhao",
    "Jiuyang Shi",
    "Junjie Wang",
    "Shuang Lyu",
    "Zezhu Zeng",
    "Shirong Liang",
    "Haikuan Dong",
    "Ligang Sun",
    "Yue Chen",
    "Zhuhua Zhang",
    "Wanlin Guo",
    "Ping Qian",
    "Jian Sun",
    "Paul Erhart",
    "Tapio Ala-Nissila",
    "Yanjing Su",
    "Zheyong Fan",
]

GLOB_STR = "*.xyz"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "keys": {"field": "keys"},
    # "value": {
    #     "energy": "energy",
    #     "virial": "virial",
    #     "forces": "force",
    # }
    # },
    "input": {
        "value": """
GGA      = PE       # Default is POTCAR
ENCUT    = 600      # Default is POTCAR
KSPACING = 0.2      # Default is 0.5
KGAMMA   = .TRUE.   # This is the default
NELM     = 120      # Default is 60
ALGO     = Normal   # This is the default
EDIFF    = 1E-06    # Default is 1E-4
SIGMA    = 0.02     # Default is 0.2
ISMEAR   = 0        # Default is 1
PREC     = Accurate # Default is Normal
LREAL    = A        # Default is .FALSE.
            """
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
            "forces": {"field": "force", "units": "eV/angstrom"},
        },
    ],
    cauchy_stress_pd["property-name"]: [
        {
            "stress": {"field": "virial", "units": "kbar"},
            "volume-normalized": {"value": True, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}

DSS = [
    (
        "DS_14h4rvviya0k_0",
        "10.60732/23c88dd7",
        DATASET_FP / "trainset/combined-train-xyz",
        "The training set for UNEP-v1 (version 1 of Unified NeuroEvolution Potential), a model implemented in GPUMD.",  # noqa
    ),
    (
        "DS_jyklsju4h580_0",
        None,
        DATASET_FP / "testset",
        "The test set for UNEP-v1 (version 1 of Unified NeuroEvolution Potential), a model implemented in GPUMD.",  # noqa
    ),
]


def reader(fp):
    for i, c in enumerate(iread(fp, index=":", format="extxyz")):
        c.info["_name"] = f"unep_v1__{'_'.join(fp.parts[-3:-1])}_{fp.stem}__index_{i}"
        if c.arrays.get("forces") is not None:
            c.info["force"] = c.arrays["forces"]
            c.info["keys"] = {
                "energy": "energy",
                "virial": "virial",
                "forces": "forces",
            }
        else:
            c.info["keys"] = {
                "energy": "energy",
                "virial": "virial",
                "forces": "force",
            }
        yield AtomicConfiguration.from_ase(c)


def wrapper(dir_path: Path):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        raise FileNotFoundError(f"{dir_path} does not exist")
    for f in dir_path.rglob("*.xyz"):
        for config in reader(f):
            yield config


def main():
    for i, (ds_id, doi, dataset_fp, ds_description) in enumerate(DSS):
        beg = time()
        config_generator = wrapper(dataset_fp)
        dm = DataManager(
            nprocs=1,
            configs=config_generator,
            prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
            prop_map=PROPERTY_MAP,
            dataset_id=ds_id,
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
            description=ds_description,
            publication_year=PUBLICATION_YEAR,
            doi=doi,
            # labels=LABELS,
        )
        print(f"Time to create dataset: {time() - t}")
        print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    main()

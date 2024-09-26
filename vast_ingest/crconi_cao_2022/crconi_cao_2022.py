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
from time import time
from functools import partial
from pathlib import Path

from dotenv import load_dotenv

from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.parsers import mlip_cfg_reader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/crconi_cao_2022/Training_Cao_20220823.cfg"  # noqa
)
DATASET_NAME = "CrCoNi_Cao_2022"
AUTHORS = ["Yifan Cao", "Killian Sheriff", "Rodrigo Freitas"]
PUBLICATION_LINK = "https://arxiv.org/abs/2311.01545"
DATA_LINK = (
    "https://github.com/yifan-henry-cao/MachineLearningPotential"
    "/blob/main/Training_datasets/Training_Cao_20220823.cfg"
)
OTHER_LINKS = ["https://github.com/yifan-henry-cao/MachineLearningPotential"]
DESCRIPTION = (
    "Training dataset that captures chemical short-range order in equiatomic "
    "CrCoNi medium-entropy alloy published with our work Quantifying chemical "
    "short-range order in metallic alloys (description provided by authors)"
)
LABELS = ["equiatomic", "short range order"]
LICENSE = "MIT"

DOI = ""
DATASET_ID = "DS_z2mkok0egrm8_0"
PUBLICATION_YEAR = "2024"

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

loader.config_table = "ndb.colabfit.dev.co_cr"
loader.prop_object_table = "ndb.colabfit.dev.po_cr"
loader.config_set_table = "ndb.colabfit.dev.cs_cr"
loader.dataset_table = "ndb.colabfit.dev.ds_cr"

# loader.config_table = "ndb.colabfit.dev.co_wip"
# loader.prop_object_table = "ndb.colabfit.dev.po_wip2"
# loader.config_set_table = "ndb.colabfit.dev.cs_wip"
# loader.dataset_table = "ndb.colabfit.dev.ds_wip2"


PI_METADATA = {
    "software": {"value": "VASP 6.2.1"},
    "method": {"value": "DFT-PBE"},
    "keys": {
        "value": {
            energy_pd["property-name"]: "Energy",
            atomic_forces_pd["property-name"]: "fx, fy, fz",
            cauchy_stress_pd["property-name"]: "PlusStress",
        }
    },
    "input": {
        "value": {
            "basis-set": "projector-augmented wave",
            "k-point-grid": "3x3x3",
            "plane-wave-cutoff": "430 eV",
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
            "stress": {"field": "stress", "units": "kbar"},
        },
    ],
    "_metadata": PI_METADATA,
}

element_map = {"0": "Cr", "1": "Co", "2": "Ni"}
reader = partial(mlip_cfg_reader, element_map)


def main():
    beg = time()
    config_generator = reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
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
        license=LICENSE,
    )
    # If running as a script, include below to stop the spark instance
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

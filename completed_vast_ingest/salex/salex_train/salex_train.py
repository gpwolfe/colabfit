"""
author: Gregory Wolfe

Properties
----------
energy
atomic-forces
stress

Other properties added to metadata
----------------------------------


File notes
----------
Due to install of fairchem (dependencies on torch, torch-geometric,
torch-scatter), I have preprocessed all files locally, where the install worked.
The data is stored in pickle files of ASE atoms objects, which are read in here.

There is an associated metadata.npz file, but this contains only the number of
atoms in each structure

Converting first to ASE atoms object allows convenient retrieval of  props through
atoms.info. Not sure where some of this info is hiding in the "AseDBDataset" object

Take all properties from the atoms.calc object. There are some other energy values in info,
but it appears these were used as a comparison to the Materials Project 2020 values

 atoms.info['energy_corrected_mp2020'], which is connected to other
properties that will be included in the metadata of the POs. For example,
 'energy_correction_uncertainty_mp2020': 0.004,
 'energy_adjustments_mp2020': ['Composition-based energy adjustment (-0.687 eV/atom x 2.0 atoms)'], # noqa
 'correction_warnings': ['Failed to guess oxidation states for Entry None (Ba6Ga2NO). Assigning anion ... # noqa

From the documentation:  total energy (eV), forces (eV/A) and stress (eV/A^3).
Thank you for these units
"""

import os
from pathlib import Path
from pickle import load
from time import time

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property import PropertyMap, property_info
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Set up data loader environment
load_dotenv()
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")
ACTUAL_INDEX = int(os.getenv("ACTUAL_INDEX"))

n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)


loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_spark_session(spark_session)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_salex"
loader.prop_object_table = "ndb.colabfit.dev.po_salex"
loader.config_set_table = "ndb.colabfit.dev.cs_salex"
loader.dataset_table = "ndb.colabfit.dev.ds_salex"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_salex"


# loader.config_table = "ndb.colabfit.dev.co_wip"
# loader.prop_object_table = "ndb.colabfit.dev.po_wip"
# loader.config_set_table = "ndb.colabfit.dev.cs_wip"
# loader.dataset_table = "ndb.colabfit.dev.ds_wip"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/salex/salex_train_pickles"  # noqa
)
DATASET_NAME = "sAlex_train"
DATASET_ID = "DS_gxjhn6vdjnxg_0"
DESCRIPTION = "The training split of sAlex. sAlex is a subsample of the Alexandria dataset that was used to fine tune the OMat24 (Open Materials 2024) models. From the site: sAlex was created by removing structures matched in WBM and only sampling structure along a trajectory with an energy difference greater than 10 meV/atom."  # noqa


DOI = None


PUBLICATION_YEAR = "2024"
AUTHORS = [
    "Luis Barroso-Luque",
    "Muhammed Shuaibi",
    "Xiang Fu",
    "Brandon M. Wood",
    "Misko Dzamba",
    "Meng Gao",
    "Ammar Rizvi",
    "C. Lawrence Zitnick",
    "Zachary W. Ulissi",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.48550/arXiv.2410.12771"
DATA_LINK = "https://fair-chem.github.io/core/datasets/omat24.html"
OTHER_LINKS = [
    "https://doi.org/10.1002/adma.202210788",
    "https://huggingface.co/datasets/fairchem/OMAT24",
    "https://alexandria.icams.rub.de/",
]

property_map = PropertyMap([atomic_forces_pd, energy_pd, cauchy_stress_pd])
property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "PBE+U")
input_ = "From the publication: 1. Version 54 of pseudopoentials provided by VASP were used, rather than the legacy PBE MPRelaxSet defaults. The Yb_3 and W_sv pseudopotentials were used for Yb and W to account for changes between version 52 and 54 of VASP PBE pseudopotentials. 2. All calculations were done with the ALGO flag set to Normal"  # noqa
property_map.set_metadata_field("input", input_)

energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)
stress_info = property_info(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info])

PROPERTY_MAP = property_map.get_property_map()


def omat_reader(fp):
    with open(fp, "rb") as f:
        atoms = load(f)
    for ix, config in enumerate(atoms):
        yield AtomicConfiguration.from_ase(config)


def reader_wrapper(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    all_paths = sorted(
        list(dir_path.rglob("*.pkl")),
        key=lambda x: x.stem,
    )
    if ACTUAL_INDEX >= len(all_paths):
        print(f"Index {ACTUAL_INDEX} out of range of path list")
        return
    path = all_paths[ACTUAL_INDEX]
    print(f"slurm task id: {SLURM_TASK_ID}")
    print(f"ACTUAL_INDEX: {ACTUAL_INDEX}")
    print(f"Processing file: {path}")
    reader = omat_reader(path)
    for config in reader:
        yield config


def main():
    t0 = time()
    config_generator = reader_wrapper(DATASET_FP)
    print("Creating DataManager")
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=10000,
    )
    print(f"Time to prep: {time() - t0}")
    t1 = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=True)
    t2 = time()
    print(f"Time to load: {t2 - t1}")
    print(f"Total time: {time() - t0}")
    print("complete")


if __name__ == "__main__":
    main()

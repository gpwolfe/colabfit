"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------


File notes
----------
json files
contains dict where keys = material id
each material id contains list of dicts with (examples):
'kpoints': [7, 2, 2],
'PREC': 'high',
'ENMAX': 264.4175,
'ENAUG': 446.842,
'steps': [this is a list of dicts]

each step contains keys:
['structure', 'energy', 'forces', 'stress']
where structure is a pymatgen.core.structure.Structure object

an ASE Atoms object can be created as follows
Structure.from_dict(data["agm003272798"][1]["steps"][0]["structure"]).to_ase_atoms()

stress units are assumed to be eV/angstrom^3, as pymatgen appears to use
these by default
"""

import os
from pathlib import Path
from pickle import load
from time import time

import pyspark
from colabfit.tools.configuration import AtomicConfiguration  # noqa
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property import PropertyMap, property_info
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from dotenv import load_dotenv
from pyspark.sql import SparkSession

print("pyspark version: ", pyspark.__version__)
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
    .config("spark.ui.showConsoleProgress", "true")
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
loader.config_table = "ndb.colabfit.dev.co_alex"
loader.prop_object_table = "ndb.colabfit.dev.po_alex"
loader.config_set_table = "ndb.colabfit.dev.cs_alex"
loader.dataset_table = "ndb.colabfit.dev.ds_alex"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_alex"


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
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alexandria_3d_pbe_traj/alexandria_pickles"  # noqa
)
DATASET_NAME = "Alexandria_geometry_optimization_paths_PBE_3D"
DATASET_ID = "DS_s6gf4z2hcjqy_0"
DESCRIPTION = "The Alexandria Materials Database contains theoretical crystal structures in 1D, 2D and 3D discovered by machine learning approaches using DFT with PBE, PBEsol and SCAN methods. This dataset represents the geometry optimization paths for 3D crystal structures from Alexandria calculated using PBE methods."  # noqa

DOI = None
PUBLICATION_YEAR = "2024"
AUTHORS = [
    "Jonathan Schmidt",
    "Noah Hoffmann",
    "Hai-Chen Wang",
    "Pedro Borlido",
    "Pedro J. M. A. Carriço",
    "Tiago F. T. Cerqueira",
    "Silvana Botti",
    "Miguel A. L. Marques",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.1002/adma.202210788"
DATA_LINK = "https://alexandria.icams.rub.de/"


property_map = PropertyMap([atomic_forces_pd, energy_pd, cauchy_stress_pd])
property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "PBE")
property_map.set_metadata_field("input", "input", dynamic=True)

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


def read_alexandria(fp: Path):
    with fp.open("rb") as f:
        for config in load(f):
            yield config


# cs_name_match = f"alexandria_3d__file_{fp.stem}__id_{id}__trajectory_{j}"
# cs_label_match = None
# cs_name = f"{DATASET_NAME}__{fp.stem}__{id}__{j}"
# cs_description = f"Alexandria 3D PBE trajectory number {j} for material {id} from file {fp.name}"  # noqa
# CSS[cs_name] = {}
# CSS[cs_name]["name"] = cs_name
# CSS[cs_name]["description"] = cs_description
# CSS[cs_name]["ids"] = []
# (cs_name_match, cs_label_match, cs_name, cs_description)]


def read_wrapper(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    all_json_paths = sorted(list(dir_path.rglob("*.pkl")), key=lambda x: x.stem)
    print(f"slurm task id: {SLURM_TASK_ID}")
    print(f"ACTUAL_INDEX: {ACTUAL_INDEX}")
    if ACTUAL_INDEX > len(all_json_paths):
        print("ACTUAL_INDEX is greater than the number of files")
        return
    path = all_json_paths[ACTUAL_INDEX]
    css_dir = Path("alexandria_css")
    css_dir.mkdir(exist_ok=True)
    css_path = css_dir / f"{path.stem}.csv"
    with css_path.open("a") as f:
        f.write("cs_name,config_id\n")
        print(f"Processing file: {path}")
        reader = read_alexandria(path)
        for config in reader:
            if config.info["_name"] != [
                "alexandria_3d__file_alex_go_aad_020__id_agm005676148__trajectory_0__frame_5"  # noqa
            ]:
                _, filename, mat_id, traj, ix = config.info["_name"][0].split("__")
                cs_name = f"{DATASET_NAME}__{filename}__{mat_id}__{traj}"
                f.write(f"{cs_name},{config.id}\n")
                yield config


def main():
    t0 = time()
    config_generator = read_wrapper(DATASET_FP)
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

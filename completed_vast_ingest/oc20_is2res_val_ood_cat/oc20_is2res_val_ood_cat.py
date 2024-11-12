"""
author: Gregory Wolfe

Properties
----------
potential energy

Other properties added to metadata
----------------------------------
dipole, original scf energy without the energy correction subtracted

File notes
----------
File /scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/data/ \
    s2ef_train_20M/s2ef_train_20M/1050.extxyz is malformed, truncated

columns from txt files: system_id,frame_number,reference_energy

header from extxyz files:
Lattice=""
Properties=species:S:1:pos:R:3:move_mask:L:1:tags:I:1:forces:R:3
energy=-181.54722937
free_energy=-181.54878652
pbc="T T T"

get:
config.constraints
config.arrays (tags, forces)
config.info (energy, free_energy)

"""

import os
import pickle
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    energy_pd,
    adsorption_energy_pd,
)

print("pyspark version: ", pyspark.__version__)
# Set up data loader environment
load_dotenv()
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID", -1))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID", -1)
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
print(jars)
spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
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

# loader.metadata_dir = "test_md/MD"
# loader.config_table = "ndb.colabfit.dev.co_is2ads"
# loader.prop_object_table = "ndb.colabfit.dev.po_is2ads"
# loader.config_set_table = "ndb.colabfit.dev.cs_is2ads"
# loader.dataset_table = "ndb.colabfit.dev.ds_is2ads"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_is2ads"

loader.config_table = "ndb.colabfit.dev.co_oc_reset"
loader.prop_object_table = "ndb.colabfit.dev.po_oc_reset"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_oc_reset"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/is2res_val_ood_cat_trajectories/"  # noqa
)
DATASET_NAME = "OC20_IS2RES_val_ood_cat"
DATASET_ID = "DS_ava2xqam2xfi_0"
DOI = None

PUBLICATION_YEAR = "2024"
AUTHORS = [
    "Lowik Chanussot",
    "Abhishek Das",
    "Siddharth Goyal",
    "Thibaut Lavril",
    "Muhammed Shuaibi",
    "Morgane Riviere",
    "Kevin Tran",
    "Javier Heras-Domingo",
    "Caleb Ho",
    "Weihua Hu",
    "Aini Palizhati",
    "Anuroop Sriram",
    "Brandon Wood",
    "Junwoong Yoon",
    "Devi Parikh",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.1021/acscatal.0c04525"
DATA_LINK = "https://fair-chem.github.io/core/datasets/oc20.html"
DESCRIPTION = (
    "OC20_IS2RES_val_ood_cat is the out-of-domain validation set for the OC20 "
    "Initial Structure to Relaxed Structure (IS2RS) and Initial Structure to Relaxed "
    "Energy (IS2RE) tasks with unseen catalyst composition. "
    "Features include energy, "
    "atomic forces and data from the OC20 mappings file, including "
    "adsorbate id, materials project bulk id and miller index."
)


PKL_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/oc20_data_mapping.pkl"  # noqa
)

with open(DATASET_FP / "system.txt", "r") as f:
    ref_text = [x.strip().split(",") for x in f.readlines()]
    REF_E_DICT = {k: float(v) for k, v in ref_text}

with open(PKL_FP, "rb") as f:
    OC20_MAP = pickle.load(f)

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-rPBE"},
    "basis_set": {"value": "def2-TZVPP"},
    "input": {
        "value": {
            "EDIFFG": "1E-3",
        },
    },
    "property_keys": {
        "value": {
            "energy": "free_energy",
            "atomic_forces": "forces",
            "adsorption_energy": "free_energy - reference_energy",
        }
    },
    "reference_energy_for_adsorption_energy": {
        "field": "reference_energy",
    },
}


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    adsorption_energy_pd["property-name"]: [
        {
            "energy": {"field": "adsorption_energy", "units": "eV"},
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

CO_METADATA = {
    key: {"field": key}
    for key in [
        "constraints",
        "bulk_id",
        "ads_id",
        "bulk_mpid",
        "bulk_symbols",
        "ads_symbols",
        "miller_index",
        "shift",
        "top",
        "adsorption_site",
        "class",
        "anomaly",
        "system_id",
        "frame_number",
    ]
}


def oc_reader(fp: Path):
    oc_id = fp.stem
    iter_configs = iread(fp, format="extxyz", index=":")
    for i, config in enumerate(iter_configs):
        try:
            reference_energy = REF_E_DICT[oc_id]
            config.info["constraints-fix-atoms"] = config.constraints[0].index
            config_data = OC20_MAP[oc_id]
            config.info.update(config_data)
            config.info["reference_energy"] = reference_energy
            config.info["adsorption_energy"] = (
                config.info["free_energy"] - reference_energy
            )
            config.info["system_id"] = oc_id
            config.info["frame_number"] = str(i)
            config.info["_name"] = f"{DATASET_NAME}__file_{oc_id}__config_{i}"
            config.info["_labels"] = [
                f"frame:{i}",
                f"open_catalyst_id:{oc_id}",
                f"materials_project_id:{config_data['bulk_mpid']}",
                f"file:{fp.name}",
            ]
            yield AtomicConfiguration.from_ase(config, CO_METADATA)
        except Exception as e:
            print("Error encountered: ", e)
            continue


def oc_wrapper(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    xyz_paths = sorted(
        list(dir_path.rglob("*.extxyz")),
        key=lambda x: int(x.stem.replace("random", "")),
    )
    step_size = 10
    xyz_range = (SLURM_TASK_ID * step_size, (SLURM_TASK_ID + 1) * step_size)
    # if SLURM_TASK_ID > len(xyz_paths):
    #     raise ValueError(f"This task ID {SLURM_TASK_ID} greater than number of files")
    # xyz_path = xyz_paths[SLURM_TASK_ID]
    # for xyz_path in xyz_paths:
    #     print(f"Reading {xyz_path.name}")
    if xyz_range[0] >= len(xyz_paths):
        return
    if xyz_range[1] > len(xyz_paths):
        xyz_range = (xyz_range[0], len(xyz_paths))
    xyz_paths = xyz_paths[xyz_range[0] : xyz_range[1]]  # noqa
    print(f"slurm task id: {SLURM_TASK_ID}")
    for path in xyz_paths:
        print(f"Processing file: {path}")
        reader = oc_reader(path)
        for config in reader:
            yield config


def main():
    beg = time()
    config_generator = oc_wrapper(DATASET_FP)
    print("Creating DataManager")
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, adsorption_energy_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=10000,
    )
    print(f"Time to prep: {time() - beg}")
    t = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=True)
    print(f"Time to load: {time() - t}")
    print("complete")
    # labels = ["OC20", "Open Catalyst"]
    # print("Creating dataset")
    # t = time()
    # dm.create_dataset(
    #     loader,
    #     name=DATASET_NAME,
    #     authors=AUTHORS,
    #     publication_link=PUBLICATION,
    #     data_link=DATA_LINK,
    #     data_license=LICENSE,
    #     description=DESCRIPTION,
    #     publication_year=PUBLICATION_YEAR,
    #     labels=labels,
    # )
    # print(f"Time to create dataset: {time() - t}")
    # loader.stop_spark()
    # print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    # print(range)
    # main(range)
    main()

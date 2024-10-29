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
spark_session = SparkSession.builder.appName(
    f"cf_oc20_valid_{SLURM_JOB_ID}_{SLURM_TASK_ID}"
).getOrCreate()

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
# loader.config_table = "ndb.colabfit.dev.co_oodsads"
# loader.prop_object_table = "ndb.colabfit.dev.po_oodsads"
# loader.config_set_table = "ndb.colabfit.dev.cs_oodsads"
# loader.dataset_table = "ndb.colabfit.dev.ds_oodsads"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_oodsads"


loader.config_table = "ndb.colabfit.dev.co_oc_reset"
loader.prop_object_table = "ndb.colabfit.dev.po_oc_reset"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_oc_reset"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/s2ef_val_ood_ads/"
)
DATASET_NAME = "OC20_S2EF_val_ood_ads"
DATASET_ID = "DS_cgdsc3gxoamu_0"
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
    "OC20_S2EF_val_ood_ads is the out-of-domain validation set of the OC20 "
    "Structure to Energy and Forces (S2EF) dataset featuring unseen adsorbate. "
    "Features include energy, "
    "atomic forces and data from the OC20 mappings file, including "
    "adsorbate id, materials project bulk id and miller index."
)


PKL_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/oc20_data_mapping.pkl"  # noqa
)
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
            # "reference-energy": {"field": "reference_energy", "units": "eV"},
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
    fp_num = f"{int(fp.stem):04d}"
    prop_fp = fp.with_suffix(".txt")
    with prop_fp.open("r") as prop_f:
        prop_lines = [x.strip() for x in prop_f.readlines()]
        iter_configs = iread(fp, format="extxyz", index=":")
        for i, config in enumerate(iter_configs):
            try:
                system_id, frame_number, reference_energy = prop_lines[i].split(",")
                reference_energy = float(reference_energy)
                config.info["constraints-fix-atoms"] = config.constraints[0].index
                config_data = OC20_MAP[system_id]
                config.info.update(config_data)
                config.info["reference_energy"] = reference_energy
                config.info["adsorption_energy"] = (
                    config.info["free_energy"] - reference_energy
                )
                config.info["system_id"] = system_id
                config.info["frame_number"] = frame_number.replace("frame", "")
                config.info["_name"] = f"{DATASET_NAME}__file_{fp_num}__config_{i}"
                config.info["_labels"] = [
                    f"frame:{frame_number}",
                    f"system:{system_id}",
                    f"materials_project_id:{config_data['bulk_mpid']}",
                    f"file:{fp.name}",
                ]
                yield AtomicConfiguration.from_ase(config, CO_METADATA)
            except Exception as e:
                print("Error encountered: ", e)
                continue


def oc_wrapper(dir_path: str):
    # slurm_task_count = int(os.getenv("SLURM_ARRAY_TASK_COUNT", 1))
    # slurm_job_id = os.getenv("SLURM_JOB_ID", -1)
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    xyz_paths = sorted(
        list(dir_path.rglob("*.extxyz")),
        key=lambda x: int(x.stem),
    )
    if SLURM_TASK_ID > len(xyz_paths):
        raise ValueError(f"This task ID {SLURM_TASK_ID} greater than number of files")
    xyz_path = xyz_paths[SLURM_TASK_ID]
    reader = oc_reader(xyz_path)
    for config in reader:
        yield config


def main():
    beg = time()
    print(beg)
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
    # labels = ["OC20", "Open Catalyst"]
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
    )
    print("complete")

    print(f"Time to create dataset: {time() - t}")
    # loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    # print(range)
    # main(range)
    main()

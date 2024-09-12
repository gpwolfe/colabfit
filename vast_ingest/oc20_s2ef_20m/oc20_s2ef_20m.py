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

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd


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
# loader.config_table = "ndb.`colabfit-prod`.prod.co_20240820"
loader.config_table = "ndb.`colabfit-prod`.prod.co"
loader.config_set_table = "ndb.`colabfit-prod`.prod.cs"
# loader.dataset_table = "ndb.`colabfit-prod`.prod.ds_20240820"
loader.dataset_table = 'ndb.`colabfit-prod`.prod.ds'
loader.prop_object_table = "ndb.`colabfit-prod`.prod.po"
# loader.config_table = "ndb.colabfit.dev.co_oc20_test4"
# loader.config_set_table = "ndb.colabfit.dev.cs_oc20_test4"
# loader.dataset_table = "ndb.colabfit.dev.ds_oc20_test4"
# loader.prop_object_table = "ndb.colabfit.dev.po_oc20_test4"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

# DATASET_FP = Path(
#     "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/data/s2ef_train_20M/s2ef_train_20M/"
# )
DATASET_FP = Path("/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/oc20_20m/1600_missing.extxyz")
DATASET_NAME = "OC20_S2EF_train_20M"

ds_id = "DS_otx1qc9f3pm4_0"

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
DATA_LINK = (
    "https://fair-chem.github.io/core/datasets/oc20.html"
)
DESCRIPTION = (
    "OC20_S2EF_train_20M is the 20 million structure training subset of the OC20 "
    "Structure to Energy and Forces dataset. Features include potential energy, "
    "free energy and atomic forces. Data from the OC20 mappings file, including "
    "adsorbate id, materials project bulk id, miller index, shift and others."
)


PKL_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/"
    "gw_scripts/oc20_s2ef/data/oc20_data_mapping.pkl"
)
with open(PKL_FP, "rb") as f:
    OC20_MAP = pickle.load(f)

# GLOB_STR = "*.extxyz"
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
        "energy": "free_energy",
        "atomic_forces": "forces",
    },
}


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "reference_energy", "units": "eV"},
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
    # fp_num = f"{int(fp.stem):04d}"
    fp_num = "1600"
    # prop_fp = fp.with_suffix(".txt")
    prop_fp = Path("/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/data/s2ef_train_20M/s2ef_train_20M/1600.txt")
    with prop_fp.open("r") as prop_f:
        prop_lines = [x.strip() for x in prop_f.readlines()][-525:]
        iter_configs = iread(fp, format="extxyz", index=":")
        i = 0
        for i, config in enumerate(iter_configs):
            try:
                system_id, frame_number, reference_energy = prop_lines[i].split(",")
                reference_energy = float(reference_energy)
                config.info["constraints-fix-atoms"] = config.constraints[0].index
                config_data = OC20_MAP[system_id]
                config.info.update(config_data)
                config.info["reference_energy"] = reference_energy
                config.info["system_id"] = system_id
                config.info["frame_number"] = frame_number
                # config.info["forces"] = forces[i]
                config.info["_name"] = f"{DATASET_NAME}__file_{fp_num}_config_{i}"
                yield AtomicConfiguration.from_ase(config, CO_METADATA)
            except Exception as e:
                print("Error encountered: ", e)
                continue
            # except StopIteration:
            #     break


def oc_wrapper(dir_path: str, range: tuple):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    xyz_paths = sorted(
        # list(dir_path.rglob("*.extxyz")),
        list(dir_path.glob("1060_missing.extxyz")),
        key=lambda x: f"{int(x.stem):05d}",
    )
    if range[1] == -1:
        range = (range[0], len(xyz_paths))
    xyz_paths = xyz_paths[range[0] : range[1]]  # noqa E203
    print(f"{range[0]} to {range[1]}")
    for xyz_path in xyz_paths:
        print(f"Reading {xyz_path.name}")
        reader = oc_reader(xyz_path)
        for config in reader:
            yield config


def oc_wrapper_wrapper(dir_path: str, range: tuple):
    yield from oc_wrapper(dir_path, range)


def main(range: tuple):
    beg = time()
    # loader.zero_multiplicity(ds_id)
    # config_generator = oc_wrapper(DATASET_FP, range)
    config_generator = oc_reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=ds_id,
        standardize_energy=True,
        read_write_batch_size=100000,
    )
    print(f"Time to prep: {time() - beg}")
    # t = time()
    # dm.load_co_po_to_vastdb(loader, batching_ingest=True)
    # print(f"Time to load: {time() - t}")
    labels = ["OC20", "Open Catalyst"]
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
        labels=labels,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    # print(range)
    # main(range)
    main((0, 1))

    

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

import functools
import json
import logging
import pickle
import sys
import time
from argparse import ArgumentParser
from functools import partial
from pathlib import Path

import pymongo
from ase.io import iread
from tqdm import tqdm

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/oc20_s2ef/s2ef_train_20M/s2ef_train_20M/")
ELEMENTS = None

PKL_FP = Path("data/oc20_s2ef/oc20_data_mapping.pkl")
with open(PKL_FP, "rb") as f:
    OC20_MAP = pickle.load(f)
GLOB_STR = "*.extxyz"
PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-rPBE"},
    "basis_set": {"value": "def2-TZVPP"},
    "input": {
        "value": {
            "EDIFFG": "1E-3",
        },
    },
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "reference-energy": {"field": "reference_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "free-energy": [
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

MAX_AUTO_RECONNECT_ATTEMPTS = 100


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                if wait_t > 1800:
                    wait_t = 1800  # cap at 1/2 hour
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


def oc_reader(fp: Path, ds_name: str):
    fp_num = f"{int(fp.stem):04d}"
    prop_fp = fp.with_suffix(".txt")
    configs = []
    with prop_fp.open("r") as prop_f:
        prop_lines = [x.strip() for x in prop_f.readlines()]

        iter_configs = iread(fp, format="extxyz")
        for i, config in tqdm(enumerate(iter_configs)):
            system_id, frame_number, reference_energy = prop_lines[i].split(",")
            reference_energy = float(reference_energy)
            config.info["constraints-fix-atoms"] = config.constraints[0].index
            config_data = OC20_MAP[system_id]
            config.info.update(config_data)
            config.info["reference_energy"] = reference_energy
            config.info["system_id"] = system_id
            config.info["frame_number"] = frame_number
            # config.info["forces"] = forces[i]
            config.info["name"] = f"{ds_name}__file_{fp_num}"
            configs.append(config)
            if len(configs) == 50000:
                for config in configs:
                    yield config

                    configs = []
    if len(configs) > 0:
        for config in configs:
            yield config


@auto_reconnect
def read_wrapper(dbname, uri, nprocs, ds_id, ds_name):
    wrap_time = time.time()
    client = MongoDatabase(dbname, uri=uri, nprocs=nprocs)
    ids = []
    fps = sorted(list(DATASET_FP.rglob(GLOB_STR)))
    today = time.strftime("%Y-%m-%d")
    for fp in fps[213:]:
        fp_num = f"{int(fp.stem):03d}"

        insert_time = time.time()
        reader_part = partial(oc_reader, ds_name=ds_name)
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader_part,
            glob_string=fp.name,
            verbose=True,
            generator=True,
        )
        ids_batch = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=True,
                verbose=True,
            )
        )
        ids.extend(ids_batch)
        new_insert_time = time.time()
        print(f"Time to insert: {new_insert_time - insert_time}")

        co_ids, do_ids = list(zip(*ids_batch))
        file_ds_name = ds_name.lower().replace("-", "_")
        co_id_file = Path(
            f"{ds_id}_{file_ds_name}_co_ids_{today}/"
            f"{ds_id}_config_ids_batch_natoms_{fp_num}.txt"
        )
        co_id_file.parent.mkdir(parents=True, exist_ok=True)
        do_id_file = Path(
            f"{ds_id}_{file_ds_name}_do_ids_{today}/"
            f"{ds_id}_do_ids_batch_natoms_{fp_num}.txt"
        )
        do_id_file.parent.mkdir(parents=True, exist_ok=True)
        with open(co_id_file, "a") as f:
            f.writelines([f"{id}\n" for id in co_ids])
        with open(do_id_file, "a") as f:
            f.writelines([f"{id}\n" for id in do_ids])

    print(f"Time to read: {time.time() - wrap_time}")
    client.close()
    return ids


@auto_reconnect
def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )

    parser.add_argument(
        "-f",
        "--ds_data",
        type=Path,
        help="File of dataset data",
    )

    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    with open(args.ds_data, "r") as f:
        ds_data = json.load(f)
    ds_id = ds_data["dataset_id"]

    ds_name = ds_data["dataset_name"]

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.close()
    ids = []

    ids = read_wrapper(
        dbname=client.database_name,
        uri=client.uri,
        nprocs=client.nprocs,
        ds_id=ds_id,
        ds_name=ds_name,
    )
    print("Num. Ids: ", len(ids))


if __name__ == "__main__":
    main(sys.argv[1:])

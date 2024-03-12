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
import logging
import pickle
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

import pymongo
from ase.io import iread
from tqdm import tqdm

from colabfit.tools.database import load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)

DATASET_FP = Path("data/oc20_s2ef/s2ef_train_200K/s2ef_train_200K")
DATASET_NAME = "OC20_S2EF_train_200K"

LICENSE = "https://creativecommons.org/licenses/by/4.0/legalcode"

PUBLICATION = "https://doi.org/10.1021/acscatal.0c04525"
DATA_LINK = (
    "https://github.com/Open-Catalyst-Project/ocp/blob"
    "/main/DATASET.md#open-catalyst-2020-oc20"
)

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
DATASET_DESC = (
    "OC20_S2EF_train_200K is the 200K subset of the OC20 Structure to Energy and "
    "Forces dataset. "
)
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


def oc_reader(fp: Path):
    fp_num = f"{int(fp.stem):03d}"
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
            config.info["name"] = f"{DATASET_NAME}__file_{fp_num}"
            configs.append(config)
            if len(configs) == 50000:
                for config in configs:
                    yield config

                    configs = []
    if len(configs) > 0:
        for config in configs:
            yield config


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)

    ds_id = "DS_zdy2xz6y88nl_0"

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=oc_reader,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # cs_ids = []
    # for i, (name, query, desc) in enumerate(CSS):
    #     cs_id = client.query_and_insert_configuration_set(
    #         co_hashes=all_co_ids,
    #         ds_id=ds_id,
    #         name=name,
    #         description=desc,
    #         query=query,
    #     )

    #     cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=True,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

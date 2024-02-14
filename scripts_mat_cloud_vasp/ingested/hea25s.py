"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
13.02.2024: Dataset ingest failed before dataset creation.
ds-id = DS_zlxgrekdla8l_0 is not a valid ds-id
"""

from argparse import ArgumentParser
import functools
import logging
from pathlib import Path
import time
import sys

from ase.io import read
import pymongo


# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    generate_ds_id,
    load_data,
    MongoDatabase,
)
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/surface_segregation_in_high-entropy_alloys_from_alchemical_machine_learning_"
    "dataset_hea25s"
)
DATASET_NAME = "HEA25S_high_entropy_alloys"
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "http://doi.org/10.48550/arXiv.2310.07604"
DATA_LINK = "https://doi.org/10.24435/materialscloud:ps-20"
# OTHER_LINKS = []

AUTHORS = [
    "Arslan Mazitov",
    "Maximilian A. Springer",
    "Nataliya Lopanitsyna",
    "Guillaume Fraux",
    "Sandip De",
    "Michele Ceriotti",
]
DATASET_DESC = (
    'Dataset from "Surface segregation in high-entropy alloys from alchemical '
    'machine learning: dataset HEA25S". Includes 10000 bulk HEA structures '
    "(Dataset O), 2640 HEA surface slabs (Dataset A), together with 1000 bulk and "
    "1000 surface slabs snapshots from the molecular dynamics (MD) runs (Datasets B "
    "and C), and 500 MD snapshots of the 25 elements Cantor-style alloy surface slabs. "
    "These splits, along with their respective train, test, and validation splits, are "
    "included as configuration sets."
)
ELEMENTS = None
GLOB_STR = "*.xyz"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBEsol"},
    "input": {"field": "input"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom^3"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "kilobar"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "class": {"field": "class"},
    "scale": {"field": "scale"},
    "fps_idx": {"field": "fps_idx"},
    "hea25s_name": {"field": "name"},
    "surf": {"field": "surf"},
    "crystal": {"field": "crystal"},
    "energy_sigma_towards_zero": {"field": "energy_sigma_towards_zero"},
    "vacuum, A": {"field": "vacuum, A"},
    "origin": {"field": "origin"},
}

CSS = [
    [
        f"{DATASET_NAME}_{cs[0]}_{cs[1]}",
        {"names": {"$regex": f".*{cs[0]}.*{cs[1]}.*"}},
        f"Configurations from the {cs[0]}: {cs[1]} split of {DATASET_NAME}",
    ]
    for cs in [
        ("dataset_A_surface_random", "train_step_1"),
        ("dataset_A_surface_random", "train_step_2"),
        ("dataset_A_surface_random", "train_step_3"),
        ("dataset_A_surface_random", "train_step_4"),
        ("dataset_A_surface_random", "train_step_5"),
        ("dataset_A_surface_random", "test"),
        ("dataset_A_surface_random", "val"),
        ("dataset_B_bulk_md", "train_step_1"),
        ("dataset_B_bulk_md", "train_step_2"),
        ("dataset_B_bulk_md", "train_step_3"),
        ("dataset_B_bulk_md", "train_step_4"),
        ("dataset_B_bulk_md", "train_step_5"),
        ("dataset_B_bulk_md", "test"),
        ("dataset_B_bulk_md", "val"),
        ("dataset_C_surface_md", "train_step_1"),
        ("dataset_C_surface_md", "train_step_2"),
        ("dataset_C_surface_md", "train_step_3"),
        ("dataset_C_surface_md", "train_step_4"),
        ("dataset_C_surface_md", "train_step_5"),
        ("dataset_C_surface_md", "test"),
        ("dataset_C_surface_md", "val"),
        ("dataset_D_cantor", "train"),
        ("dataset_D_cantor", "test"),
        ("dataset_D_cantor", "val"),
        ("dataset_O_bulk_random", "train"),
        ("dataset_O_bulk_random", "test"),
        ("dataset_O_bulk_random", "val"),
    ]
]


def parse_incar(fp):
    with open(fp, "r") as f:
        lines = f.readlines()
    incar = dict()
    for line in lines:
        if "=" in line:
            keyvals = line.split("=")
            key = keyvals[0].strip()
            value = "".join(keyvals[1:]).strip().split("#")[0].strip()
            incar[key] = value
    return incar


INCAR_FP = DATASET_FP / "vasp_settings" / "INCAR"
INCAR = parse_incar(INCAR_FP)


def namer(fp):
    ds_fp_str = "__".join(DATASET_FP.absolute().parts).replace("/", "")
    name = "__".join(fp.absolute().parts).replace(ds_fp_str + "__", "").replace("/", "")
    return name


def reader(filepath: Path):
    configs = read(filepath, index=":")
    name = namer(filepath)
    for i, config in enumerate(configs):
        config.info["use_name"] = f"{name}__{config.info.get('name', i)}"
        config.info["input"] = INCAR
    return configs


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
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(free_energy_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="use_name",
        elements=ELEMENTS,
        reader=reader,
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
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_ids = []
    for i, (name, query, desc) in enumerate(CSS):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=query,
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

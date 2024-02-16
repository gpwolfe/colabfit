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
    "data/modeling_high-entropy_transition-metal_alloys_with_"
    "alchemical_compression_dataset_hea25"
)
DATASET_NAME = "HEA25_high_entropy_transition-metal_alloys"
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "http://doi.org/10.48550/arXiv.2212.13254"
DATA_LINK = "https://doi.org/10.24435/materialscloud:73-yn"
# OTHER_LINKS = []

AUTHORS = [
    "Nataliya Lopanitsyna",
    "Guillaume Fraux",
    "Maximilian A. Springer",
    "Sandip De",
    "Michele Ceriotti",
]
DATASET_DESC = (
    'Dataset from "Modeling high-entropy transition-metal alloys with alchemical '
    'compression". Includes 25,000 structures utilized for fitting the aforementioned '
    "potential, with a focus on 25 d-block transition metals, excluding Tc, Cd, Re, "
    'Os and Hg. Each configuration includes a "class" field, '
    "indicating the crystal class of the structure. The class represents the "
    "following: 1: perfect crystals; 3-8 elements per structure, "
    r"2: shuffled positions (standard deviation 0.2\AA ); 3-8 elements per structure, "
    r"3: shuffled positions (standard deviation 0.5\AA ); 3-8 elements per structure, "
    r"4: shuffled positions (standard deviation 0.2\AA ); 3-25 elements per structure. "
    "Configuration sets include divisions into fcc and bcc crystals, further split "
    "by class as described above."
)
ELEMENTS = None
GLOB_STR = "*.extxyz"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBEsol"},
    "input": {
        "value": {
            "ENCUT": 550,
            "k-points-scheme": "gamma-centered Monkhorst-Pack",
            "k-points-density": "0.04*pi*angstrom^−1 along reciprocal lattice vector",
        }
    },
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
cs_pairs = [("fcc", cclass) for cclass in range(1, 5)]
cs_pairs += [("bcc", cclass) for cclass in range(1, 5)]
CSS = [
    [
        f"{DATASET_NAME}_{cs[0]}_crystal_class_{cs[1]}",
        {"names": {"$regex": f"{cs[0]}_class_{cs[1]}"}},
        f"{cs[0]} crystal, class {cs[1]} configurations from {DATASET_NAME}",
    ]
    for cs in cs_pairs
]


def reader(filepath: Path):
    configs = read(filepath, index=":")
    # name = namer(filepath)
    for i, config in enumerate(configs):
        config.info["use_name"] = (
            f"{config.info['crystal']}_class_{config.info['class']}__{i}"
        )
        # config.info["input"] = INCAR
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

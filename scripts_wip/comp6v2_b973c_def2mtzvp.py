"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
same h5 file setup as ANI2x
"""

import functools
import logging
import sys
import h5py
import time
from argparse import ArgumentParser
from pathlib import Path

import pymongo

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, generate_ds_id, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)

DATASET_FP = Path("data/comp6_datasets")
DATASET_NAME = "COMP6v2-B973c-def2mTZVP"
LICENSE = "CC-BY-4.0"  # Creative Commons Attribution 4.0 International
PUB_YEAR = "2023"

PUBLICATION = "https://doi.org/10.1021/acs.jctc.0c00121"
DATA_LINK = "https://doi.org/10.5281/zenodo.10126157"
# OTHER_LINKS = []

AUTHORS = [
    "Kate Huddleston",
    "Roman Zubatyuk",
    "Justin Smith",
    "Adrian Roitberg",
    "Olexandr Isayev",
    "Ignacio Pickering",
    "Christian Devereux",
    "Kipton Barros",
]

DATASET_DESC = (
    "COMP6v2-B973c-def2mTZVP is the portion of COMP6v2 calculated at the "
    "B973c/def2mTZVP level of theory. "
    "COmprehensive Machine-learning Potential (COMP6) Benchmark Suite "
    "version 2.0 is an extension of the COMP6 benchmark found in "
    "the following repository: https://github.com/isayev/COMP6. COMP6v2 is a data set "
    "of density functional properties for molecules containing H, C, N, O, S, F, and "
    "Cl. It is available at the following levels of theory: wB97X/631Gd (data used to "
    "train model in the ANI-2x paper); wB97MD3BJ/def2TZVPP; wB97MV/def2TZVPP; "
    "B973c/def2mTZVP. The 6 subsets from COMP6 (ANI-MD, DrugBank, GDB07to09, GDB10to13 "
    "Tripeptides, and s66x8) are contained in each of the COMP6v2 datasets "
    "corresponding to the above levels of theory."
)
ELEMENTS = None
GLOB_STR = "COMP6v2-B973c-def2mTZVP.h5"

PI_METADATA = {
    "software": {"value": "ORCA 4.2.1"},
    "method": {"value": "DFT-B973c"},
    "basis-set": {"value": "def2m-TZVP"},
    "input": {
        "value": "! b97-3c engrad ScfConvForced tightscf\n"
        "%elprop dipole true quadrupole true end\n"
        "%output PrintLevel mini Print[P DFTD GRAD] 1 end"
    },
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "reference-energy": {"field": "en_correction", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "hartree/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "eV/angstrom^3"},
    #         "volume-normalized": {"value": False, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

CO_METADATA = {
    "dipole": {"field": "dipole", "units": "electron angstrom"},
    "force-corrections": {"field": "force_corrections", "units": "hartree/angstrom"},
}

# CSS = [
#     [
#         f"{DATASET_NAME}_aluminum",
#         {"names": {"$regex": "aluminum"}},
#         f"Configurations of aluminum from {DATASET_NAME} dataset",
#     ]
# ]


def ani_reader(fp):
    with h5py.File(fp) as h5:
        for num_atoms in h5.keys():
            # print(num_atoms)
            properties = h5[str(num_atoms)]
            coordinates = properties["coordinates"]
            reference_en = properties["D3.energy-corrections"]
            force_corrections = properties["D3.force-corrections"]
            # scf_en = properties["wB97M_def2-TZVPP.scf-energies"]
            species = properties["species"]
            energies = properties["energies"]
            dipoles = properties["dipole"]
            forces = properties["forces"]
            configs = []
            while True:
                for i, coord in enumerate(coordinates):
                    config = AtomicConfiguration(
                        positions=coord,
                        numbers=species[i],
                    )
                    config.info["energy"] = energies[i]
                    config.info["en_correction"] = reference_en[i]
                    # config.info["scf_energy"] = scf_en[i]
                    config.info["dipole"] = dipoles[i]
                    config.info["forces"] = forces[i]
                    config.info["force_corrections"] = force_corrections[i]
                    config.info["name"] = f"{DATASET_NAME}__natoms_{num_atoms}__ix_{i}"
                    configs.append(config)
                    if len(configs) == 50000:
                        for config in configs:
                            yield config
                        configs = []
                if len(configs) > 0:
                    for config in configs:
                        yield config
                    break


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
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()
    print(f"Dataset ID: {ds_id}\nDS Name: {DATASET_NAME}")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=ani_reader,
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
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        other_links=None,
        description=DATASET_DESC,
        publication_year=PUB_YEAR,
        verbose=True,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

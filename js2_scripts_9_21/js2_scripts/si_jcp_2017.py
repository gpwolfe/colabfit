#!/usr/bin/env python
# coding: utf-8


"""
author:

Data can be downloaded from:


Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
From Dylan's unfinished script

Data being uploaded from Si_md.extxyz is half of a dataset obtained
from a paper called 'Sensitivity and Dimensionality...' by Onat, Ortner, andn Kermode
( https://aip.scitation.org/doi/10.1063/5.0016005 ).
Data downloaded from this is cited as data from 'Representations in neural network
based empirical potentials' ( https://aip.scitation.org/doi/10.1063/1.4990503 ) as
a colleague said that it is the same data. I am placing this note here as since the
data in the supplementary information is not in a format I know how to read so I
cannot confirm it. If a customer messages about this data, please see this and direct
them using this info.
"""

from argparse import ArgumentParser
from ase.io import read
from pathlib import Path
import sys

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/large_data/new_raw_datasets/Si_Berk/Si_md.extxyz")
DATASET_FP = Path().cwd().parent / "data/berk_si"
DS_NAME = "Si_JCP_2017"
DS_DESC = "Silicon dataset used to train machine learning models."
AUTHORS = [
    "Ekin D. Cubuk",
    "Brad D. Malone",
    "Berk Onat",
    "Amos Waterland",
    "Efthimios Kaxiras",
]
LINKS = [
    "https://aip.scitation.org/doi/10.1063/1.4990503",
]
GLOB = "Si_md.extxyz"
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "____"},  # TODO
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "____"},  # TODO
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "____"},  # TODO
            "_metadata": {
                "software": {"value": "____"},  # TODO
            },
        }
    ],
    "kinetic-energy": [
        {
            "energy": {"field": "kinetic_energy", "units": ""},  # TODO
            "_metadata": {
                "software": {"value": ""},  # TODO
            },
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": ""},  # TODO
            "_metadata": {
                "software": {"value": ""},  # TODO
            },
        }
    ],
}


def reader(fp):
    name = fp.stem
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
    return configs


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Si"],
        verbose=True,
        generator=False,
    )

    # kinetic_energy, energy, forces, free_energy

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(free_energy_pd)

    # kinetic_energy_property_definition = {
    #     "property-id": "kinetic-energy",
    #     "property-name": "kinetic-energy",
    #     "property-title": "kinetic-energy",
    #     "property-description": "kinetic energy",
    #     "energy": {
    #         "type": "float",
    #         "has-unit": True,
    #         "extent": [],
    #         "required": True,
    #         "description": "kinetic energy",
    #     },
    # }

    # client.insert_property_definition(kinetic_energy_property_definition)

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    ds_id = client.insert_dataset(
        pr_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

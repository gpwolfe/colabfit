#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/TiO2_Berk/TiO2.extxyz"
)
DATASET = "TiO2_CMS2016"


LINKS = [
    "https://doi.org/10.1016/j.commatsci.2015.11.047",
    "https://github.com/DescriptorZoo/sensitivity-"
    "dimensionality-results/tree/master/datasets/TiO2",
]
AUTHORS = ["Nongnuch Artrith", "Alexander Urban"]
DS_DESC = (
    "TiO2 dataset that was designed to build atom neural network potentials "
    "(ANN) by Artrith et al. using the AENET package. This dataset includes "
    "various crystalline phases of TiO2 and MD data that are extracted from ab "
    "inito calculations. The dataset includes 7815 structures with 165,229 atomic "
    "environments in the stochiometric ratio of 66% O to 34% Ti."
)


def tform(c):
    c.info["per-atom"] = False


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="extxyz",
        name_field=None,
        elements=["Ti", "O"],
        default_name="TiO2",
        verbose=True,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "Quantum ESPRESSO"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "Quantum ESPRESSO"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

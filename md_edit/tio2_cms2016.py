#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import atomic_forces_pd, potential_energy_pd

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/TiO2_Berk/TiO2.extxyz"
)
DATASET_FP = Path().cwd().parent / "data/tio2_cms_2016/TiO2.extxyz"
DATASET = "TiO2_CMS2016"

PUBLICATION = "https://doi.org/10.1016/j.commatsci.2015.11.047"
DATA_LINK = (
    "https://github.com/DescriptorZoo/sensitivity-"
    "dimensionality-results/tree/master/datasets/TiO2"
)
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
PI_MD = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE"},
    "plane-wave-cutoff": {"value": "40 Ry"},
    "electron-density-cutoff": {"value": "200 Ry"},
    "pseudopotential": {"value": "GBRV ultrasoft"},
    "k-point-scheme": {"value": "gamma-centered mesh"},
    "k-point-density": {"value": "8x8x8 / 6-atom unit cell"},
    "scf-energy-convergence": {"value": "1 x 10^-6 Ry"},
    "variable-cell-opt-threshold": {"value": "0.5 kbar"},
}
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
}


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="extxyz",
        name_field=None,
        elements=["Ti", "O"],
        default_name="TiO2",
        verbose=True,
        generator=False,
    )

    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
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
        ds_id=ds_id,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

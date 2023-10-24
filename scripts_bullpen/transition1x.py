"""
author: Gregory Wolfe, Alexander Tao

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
The "all data" split, called "data", only contains 35767 configurations.
These are also all duplicates of configurations that appear in other splits
(test, train or val). There seems to be no reason to include this split.
"""
from argparse import ArgumentParser
import h5py
from pathlib import Path
import sys

from ase.atoms import Atoms

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

# DATASET_FP = Path("persistent/colabfit_raw_data/new_raw_datasets/Transition1x/")
DATASET_FP = Path().cwd() / "data"  # local
# DATASET_FP = Path().cwd()  # Greene
# DATASET_NAME = "Transition1x"
SOFTWARE = "ORCA 5.0.2"
METHODS = "DFT-wb97x"
LINKS = [
    "https://doi.org/10.1038/s41597-022-01870-w",
    "https://gitlab.com/matschreiner/Transition1x",
    "https://doi.org/10.6084/m9.figshare.19614657.v4",
]
AUTHORS = [
    "Mathias Schreiner",
    "Arghya Bhowmik",
    "Tejs Vegge",
    "Jonas Busk",
    "Ole Winther",
]


DATASET_DESC = (
    "Transition1x is a benchmark dataset containing 9.6 million Density "
    "Functional Theory (DFT) calculations of forces and energies of molecular "
    "configurations on and around reaction pathways at the ωB97x/6-31 G(d) level of "
    "theory. The configurations contained in this dataset allow a better "
    "representation of features in transition state regions when compared to other "
    "benchmark datasets -- in particular QM9 and ANI1x."
)
ELEMENTS = None
GLOB_STR = "*.h5"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "basis-set": {"value": "6-31G(d)"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_METADATA,
        }
    ],
}

# REF_ENERGIES = {
#     1: -13.62222753701504,
#     6: -1029.4130839658328,
#     7: -1484.8710358098756,
#     8: -2041.8396277138045,
#     9: -2712.8213146878606,
# }


# def get_molecular_reference_energy(atomic_numbers):
#     return sum([REF_ENERGIES[num] for num in atomic_numbers])


def generator(formula, rxn, grp, split):
    """Iterates through a h5 group"""

    energies = grp["wB97x_6-31G(d).energy"]
    forces = grp["wB97x_6-31G(d).forces"]
    atomic_numbers = list(grp["atomic_numbers"])
    positions = grp["positions"]
    # molecular_reference_energy = get_molecular_reference_energy(atomic_numbers)

    for energy, force, positions in zip(energies, forces, positions):
        config = Atoms(positions=positions, numbers=atomic_numbers)
        config.info["forces"] = force.tolist()
        config.info["energy"] = float(energy)
        config.info["name"] = f"{rxn}_{split}"
        yield config


def reader_train(fp):
    with h5py.File(fp, "r") as f:
        split = f["train"]
        for h, (formula, grp) in enumerate(split.items()):
            if h > 5:  # remove
                break  # remove
            for i, (rxn, subgrp) in enumerate(grp.items()):
                for j, config in enumerate(generator(formula, rxn, subgrp, split)):
                    yield config


def reader_test(fp):
    with h5py.File(fp, "r") as f:
        split = f["test"]
        for h, (formula, grp) in enumerate(split.items()):
            if h > 5:  # remove
                break  # remove
            for i, (rxn, subgrp) in enumerate(grp.items()):
                for j, config in enumerate(generator(formula, rxn, subgrp, split)):
                    yield config


def reader_val(fp):
    with h5py.File(fp, "r") as f:
        split = f["val"]
        for h, (formula, grp) in enumerate(split.items()):
            if h > 5:  # remove
                break  # remove
            for i, (rxn, subgrp) in enumerate(grp.items()):
                for j, config in enumerate(generator(formula, rxn, subgrp, split)):
                    yield config


# def reader_all(fp): # see file notes for reason not to include
#     with h5py.File(fp, "r") as f:
#         split = f["data"]
#         for h, (formula, grp) in enumerate(split.items()):
#             if h > 5:  # remove
#                 break  # remove
#             for i, (rxn, subgrp) in enumerate(grp.items()):
#                 for j, config in enumerate(generator(formula, rxn, subgrp, split)):
#                     yield config


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
        args.db_name,
        nprocs=args.nprocs,
        # uri=f"mongodb://{args.ip}:30007",
        uri=f"mongodb://{args.ip}:27017",
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    dss = (
        (
            "Transition1x_train",
            reader_train,
            "The training split of the Transition1x dataset. ",
        ),
        (
            "Transition1x-test",
            reader_test,
            "The test split of the Transition1x dataset. ",
        ),
        (
            "Transition1x-validation",
            reader_val,
            "The validation split of the Transition1x dataset. ",
        ),
        # ( # See file notes for reason not to include
        #     "Transition1x-all",
        #     reader_all,
        #     "All configurations from the Transition1x dataset. ",
        # ),
    )
    for name, read_function, description in dss:
        ds_id = generate_ds_id()

        configurations = list(
            load_data(
                file_path=DATASET_FP,
                file_format="folder",
                name_field="name",
                elements=ELEMENTS,
                verbose=True,
                reader=read_function,
                generator=True,
                glob_string=GLOB_STR,
            )
        )

        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=PROPERTY_MAP,
                # generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=LINKS,
            description=description + DATASET_DESC,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

"""
File notes
-----------
find better name for dataset
double-check properties/metadata
Improve dataset description
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd

DATASET_FP = Path("/large_data/new_raw_datasets_2.0/methane/methane.extxyz")
DATASET_FP = Path("data/methane/methane.extxyz")
DS_NAME = "Randomly-displaced_methane"
DS_DESC = (
    "This dataset provides a large number (7,732,488) of configurations for a simple "
    "CH4 composition that are generated in an almost completely unbiased fashion."
    "This dataset is ideal to benchmark structural representations and regression "
    "algorithms, verifying whether they allow reaching arbitrary accuracy in a data-"
    "rich regime."
)
LINKS = [
    "https://doi.org/10.1103/PhysRevLett.125.166001",
    "https://doi.org/10.1063/5.0021116",
    "https://doi.org/10.24435/materialscloud:qy-dp",
]
AUTHORS = [
    "Sergey Pozdnyakov",
    "Michael Willatt",
    "Michele Ceriotti",
]


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "Ha"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "psi4"},
                "method": {"value": "DFT-PBE"},
                "basis": {"value": "cc-pvdz"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "Ha/Bohr"},
            "_metadata": {
                "software": {"value": "psi4"},
                "method": {"value": "DFT/PBE"},
                "basis": {"value": "cc-pvdz"},
            },
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
    args = parser.parse_args(argv)

    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    ds_id = generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="extxyz",
        name_field=None,
        elements=["C", "H"],
        default_name="methane",
        verbose=True,
        generator=False,
    )

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
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

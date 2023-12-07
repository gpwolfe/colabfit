"""
File notes
-----------
Tested locally with first 100,000 index
Should work on Kubernetes
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/methane/methane")
DATASET_FP = Path("data/methane/")  # comment out, local testing
DS_NAME = "Methane_randomly_displaced"
DS_DESC = (
    "This dataset provides a large number (7,732,488) of configurations for a simple "
    "CH4 composition that are generated in an almost completely unbiased fashion."
    "Hydrogen atoms are randomly distributed in a 3A sphere centered around the carbon "
    "atom, and the only structures that are discarded are those with atoms that are "
    "closer than 0.5A, or such that the reference DFT calculation does not converge."
    "This dataset is ideal to benchmark structural representations and regression "
    "algorithms, verifying whether they allow reaching arbitrary accuracy in a data-"
    "rich regime."
)

PUBLICATION = "https://doi.org/10.1103/PhysRevLett.125.166001"
DATA_LINK = "https://doi.org/10.24435/materialscloud:qy-dp"
OTHER_LINKS = ["https://doi.org/10.1063/5.0021116"]
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
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"field": False, "units": None},
            "_metadata": {
                "software": {"value": "psi4"},
                "method": {"value": "DFT-PBE"},
                "basis": {"value": "cc-pvdz"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "hartree/bohr"},
            "_metadata": {
                "software": {"value": "psi4"},
                "method": {"value": "DFT/PBE"},
                "basis": {"value": "cc-pvdz"},
            },
        }
    ],
}


def reader(fp):
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{fp.stem}_{i}"
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    ds_id = generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        reader=reader,
        name_field="name",
        elements=["C", "H"],
        verbose=True,
        glob_string="*.extxyz",
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        # resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

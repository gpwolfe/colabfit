"""
author:gpwolfe

Data can be downloaded from:
https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0264960#sec006

File address:
https://archive.materialscloud.org/record/file?filename=training.zip&record_id=1411

Move file to script directory
mv journal.pone.0264960.s001.log $project_dir/scripts/tsff/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 tsff_plos_2022.py -i (or --ip) <database_ip>
"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path().cwd()


def reader(filepath):
    atoms = read(filepath, format="gaussian-out", index=":")
    atoms[0].info["forces"] = atoms[0].calc.results["forces"]
    atoms[0].info["energy"] = atoms[0].calc.results["energy"]
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
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
        file_format="folder",
        name_field=None,
        elements=["C", "H", "O", "N", "S"],
        reader=reader,
        glob_string="*.log",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "Gaussian 09"},
        "method": {"value": "DFT"},
    }

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "Hartree/Bohr"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        all_do_ids,
        name="TSFF_PLOS_2022",
        authors=[
            "Taylor R. Quinn",
            "Himani N. Patel",
            "Kevin H. Koh",
            "Brandon E. Haines",
            "Per-Ola Norrby",
            "Paul Helquist",
            "Olaf Wiest",
        ],
        links=[
            "https://doi.org/10.1371/journal.pone.0264960.s001",
            "https://doi.org/10.1371/journal.pone.0264960",
        ],
        description="One configuration of an enzyme: training data for "
        "a quantum-guided molecular mechanics model.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

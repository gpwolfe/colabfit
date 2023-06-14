"""
author:gpwolfe

Data can be downloaded from:
http://quantum-machine.org/datasets/
Data file address:
http://quantum-machine.org/datasets/iso17.tar.gz

Extract to data folder
tar -zxf iso17.tar.gz -C <project_dir>/scripts/iso17/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
None

File notes
----------
"""
from argparse import ArgumentParser
from ase.db import connect
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path
import sys


DB_PATH = Path().cwd()


def reader(filepath):
    filepath = Path(filepath)
    db = connect(filepath)
    atoms = []
    for row in db.select():
        atom = row.toatoms()
        atom.info = row.data
        atom.info["name"] = "iso17"
        atom.info["energy"] = row.key_value_pairs["total_energy"]
        atoms.append(atom)

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
        file_path=DB_PATH,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="*.db",
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "FHI-aims"},
        "method": {"value": "DFT-PBE"},
    }

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "atomic_forces", "units": "eV/Ang"},
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
        name="ISO17_NC_2017",
        authors=[
            "Jonathan Vandermause",
            "Yu Xie",
            "Jin Soo Lim",
            "Cameron J. Owen",
            "Boris Kozinsky",
        ],
        links=[
            "http://quantum-machine.org/datasets/",
            "https://doi.org/10.1038/s41467-019-12875-2",
            "https://proceedings.neurips.cc/paper/2017/hash/303ed4c69846ab36c2904d3ba8573050-Abstract.html",
        ],
        description="129 molecules of composition C7O2H10 from the QM9 dataset"
        " with 5000 conformational geometries apiece. Molecular dynamics data"
        " was simulated using the Fritz-Haber Institute ab initio simulation"
        " software.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

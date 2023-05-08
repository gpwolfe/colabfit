"""
author:gpwolfe

Data can be downloaded from:
https://github.com/jla-gardner/carbon-data

Clone GitHub repository (1.41 GB)
git clone https://github.com/jla-gardner/carbon-data.git
xyz files found in carbon-data/results/
or download zip file


Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
forces

Other properties added to metadata:
density
anneal temperature
gap-17-energy
timestep

File notes
----------

header from xyz file
Lattice
Properties=species:S:1:pos:R:3:
gap17_energy:R:1: <-- appears to be total energy from publication
gap17_forces:R:3
anneal_T=2000 <--anneal temperature
density=1.0
run_id=1
time=0        <--time step
pbc="T T T"
"""

from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import atomic_forces_pd
from pathlib import Path
import re
import sys

DATASET = "C_Gardner_2022"
DATASET_FP = Path().cwd()
AUTHORS = ["John L. A. Gardner", "Zoé Faure Beaulieu", "Volker L. Deringer"]
LINKS = [
    "https://github.com/jla-gardner/carbon-data",
    "https://doi.org/10.48550/arXiv.2211.16443",
]
METHODS = "DFT"
SOFTWARE = "LAMMPS, ASE"
DESCRIPTION = "Approximately 115,000 configurations of carbon with 200 \
atoms, with simulated melt, quench, reheat, then annealing \
at the noted temperature. Includes a variety of carbon \
structures."

NAME_RE = re.compile(r"density\-(?P<density>\d\.\d)\-T\-(?P<temp>\d{4}).extxyz")


def reader(file_path):
    file_name = file_path.stem
    atoms = ase.io.read(file_path, index=":", format="extxyz")
    for atom in atoms:
        atom.info["name"] = file_name
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
        name_field="name",
        elements=["C"],
        reader=reader,
        glob_string="*.extxyz",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "density": {"field": "density"},
    }
    co_md_map = {
        "anneal-temp": {"field": "anneal_T", "units": "K"},
        "gap-17-energy": {"field": "gap17_energy"},
        "timestep": {"field": "time"},
    }
    property_map = {
        "atomic-forces": [
            {
                "forces": {"field": "gap17_forces", "units": "eV/Ang"},
                "_metadata": metadata,
            }
        ]
    }
    ids = list(
        client.insert_data(
            configurations,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DESCRIPTION,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

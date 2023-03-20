"""
author:gpwolfe

Data can be downloaded from:
https://github.com/jla-gardner/carbon-data

Clone GitHub repository (1.41 GB)
git clone https://github.com/jla-gardner/carbon-data.git
xyz files found in carbon-data/results/
or download zip file
unzip carbon-data-main.zip "*xyz" -d $project_dir/scripts/c_gardner_2022

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


DATASET_FP = Path().cwd()

NAME_RE = re.compile(
    r"density\-(?P<density>\d\.\d)\-T\-(?P<temp>\d{4}).extxyz"
)


def reader(file_path):
    file_name = file_path.stem
    atoms = ase.io.read(file_path, index=":", format="extxyz")
    for atom in atoms:
        atom.info["name"] = file_name
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

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
        "software": {"value": "LAMMPS, ASE"},
        "method": {"value": "DFT, C-GAP-17"},
        "density": {"field": "density"},
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
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = []
    for fn in DATASET_FP.rglob("*.extxyz"):
        print(fn)
        groups = NAME_RE.match(fn.name).groupdict()
        print(groups)
        cs_regexes.append(
            [
                f"D_{groups['density']}_T_{groups['temp']}",
                rf"{fn.stem}",
                f"Configurations from C_gardner_2022 with "
                f"density {groups['density']} "
                f"and annealing temperature {groups['temp']}.",
            ]
        )

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={
                "hash": {"$in": all_co_ids},
                "names": {"$regex": regex},
            },
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )
        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        cs_ids=cs_ids,
        pr_hashes=all_do_ids,
        name="C_gardner_2022",
        authors="J.L.A. Gardner, Z.F. Beaulieu, V.L. Deringer",
        links=[
            "https://github.com/jla-gardner/carbon-data",
            "https://doi.org/10.48550/arXiv.2211.16443",
        ],
        description="Approximately 115,000 configurations of carbon with 200 "
        "atoms, with simulated melt, quench, reheat, then annealing "
        "at the noted temperature. Includes a variety of carbon "
        "structures.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

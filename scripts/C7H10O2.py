"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.6084/m9.figshare.c.978904.v5

Make new directory and extract files
mkdir <project_dir>/c7h10o2
tar -xf dsC7O2H10nsd.xyz.tar.bz2 -C <project_dir>/c7h10o2

Change DATASET_FP to reflect location of data parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
free energy

Other properties added to metadata:
dipole moment
isotropic polarizability
homo energy
lumo energy
homo-lumo gap energy
electronic spatial extent
zpve
internal energy at 298K
enthalpy
heat capacity

File notes
----------
"""
from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import (
    free_energy_pd,
    potential_energy_pd,
)
from collections import defaultdict
from pathlib import Path
import re
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/c7h10o2")
DATASET = "C7H10O2"
# Create custom regex parser for header and coordinate rows
parser_match = re.compile(
    r"gdb (?P<index>\d+)\s(?P<rotational_a>-?\d+\.(\d+)?)\s"
    r"(?P<rotational_b>-?\d+\.(\d+)?)\s(?P<rotational_c>-?\d+\.(\d+)?)\s"
    r"(?P<mu>-?\d+\.(\d+)?)\s(?P<alpha>-?\d+\.(\d+)?)\s"
    r"(?P<homo>-?\d+\.(\d+)?)\s(?P<lumo>-?\d+\.(\d+)?)"
    r"\s(?P<gap>-?\d+\.(\d+)?)\s(?P<r2>-?\d+\.(\d+)?)"
    r"\s(?P<zpve>-?\d+\.(\d+)?)\s(?P<u0>-?\d+\.(\d+)?)\s"
    r"(?P<u>-?\d+\.(\d+)?)\s(?P<h>-?\d+\.(\d+)?)\s"
    r"(?P<g>-?\d+\.(\d+)?)\s(?P<cv>-?\d+\.(\d+)?)"
)
# The numeric values on the xyz comment line indicate the following in this
# order, according to the README:

# index     rotational_a    rotational_b    rotational_c
# [dipole movement (mu)]      [isotropic polarizability (alpha)]
# homo      lumo    gap     [electronic spatial extent (r2)]    zpve
# [internal energy at 0K (u0)]
# [internal energy at 298K (u)]     [Enthalpy (h)]   [free energy (g)]
# [heat capacity (cv)]

coord_match = re.compile(
    r"(?P<element>[a-zA-Z]{1,2})\s+(?P<x>\S+)\s+"
    r"(?P<y>\S+)\s+(?P<z>\S+)\s+(?P<mulliken>\S+)"
)
# Format to match(spaces are tabs, no beginning space):
# C	 0.3852095134	 0.5870284364	-0.7883677644	-0.178412
# Values represent element [x y z coordinates] and partial charge (Mulliken)


def properties_parser(line):
    groups = parser_match.match(line)
    return groups.groupdict()


# Create functions to run file and heading parsers
def xyz_parser(file_path):
    file_path = Path(file_path)
    name = file_path.stem
    elem_coords = defaultdict(list)
    n_atoms = int()
    with open(file_path, "r") as f:
        line_num = 0
        for line in f:
            if line_num == 0:
                n_atoms = int(line)
                line_num += 1
            elif line_num == 1:
                property_dict = {
                    k: float(v) for k, v in properties_parser(line).items()
                }
                line_num += 1
            elif line_num < n_atoms + 2:
                if "*^" in line:
                    line = line.replace("*^", "e")
                groups = coord_match.match(line)
                try:
                    for elem_coord, val in groups.groupdict().items():
                        elem_coords[elem_coord].append(val)
                except ValueError:
                    print("ValueError at {line} in {file_path}")
                line_num += 1
            elif line_num >= n_atoms + 2:
                return name, n_atoms, elem_coords, property_dict
            else:
                print(f"{file_path} finished at line {line_num}.")
                break


def reader(file_path):
    name, n_atoms, elem_coords, properties = xyz_parser(file_path)
    positions = list(zip(elem_coords["x"], elem_coords["y"], elem_coords["z"]))
    atoms = AtomicConfiguration(
        names=[name], symbols=elem_coords["element"], positions=positions
    )
    atoms.info["name"] = name
    atoms.info["n_atoms"] = n_atoms
    for key in properties.keys():
        atoms.info[key] = properties[key]
    return [atoms]


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
        elements=["C", "O", "H"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    # Loaded from a local file of definitions

    pds = [
        free_energy_pd,
        potential_energy_pd,
    ]
    for pd in pds:
        client.insert_property_definition(pd)

    # Define properties
    metadata = {
        "software": {"value": "Gaussian 09"},
        "method": {"value": "G4MP2"},
    }
    co_md_map = {
        "homo-energy": {"field": "homo", "units": "Ha"},
        "lumo-energy": {"field": "lumo", "units": "Ha"},
        "homo-lumo-gap": {"field": "gap", "units": "Ha"},
        "polarizability": {
            "field": "alpha",
            "units": "Bohr^3",
        },
        "dipole-moment": {"field": "mu", "units": "Debye"},
        "electronic-spatial-extent": {
            "field": "r2",
            "units": "Bohr^2",
        },
        "zpve": {"field": "zpve", "units": "Ha"},
        "enthalpy": {"field": "h", "units": "Ha"},
        "heat-capacity": {
            "field": "cv",
            "units": "cal/mol K",
        },
        "internal-energy-298K": {
            "field": "u",
            "units": "Ha",
        },
    }
    property_map = {
        "free-energy": [
            {
                "energy": {"field": "g", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "potential-energy": [
            {
                "energy": {
                    "field": "u0",
                    "units": "Ha",
                },
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
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
        authors=[
            "Raghunathan Ramakrishnan",
            "Pavlo Dral",
            "Matthias Rupp",
            "O. Anatole von Lilienfeld",
        ],
        links=[
            "https://doi.org/10.6084/m9.figshare.c.978904.v5",
            "https://doi.org/10.1038/sdata.2014.22",
        ],
        description="6095 isomers of C7O2H10. Energetics were calculated"
        " at the G4MP2 level of theory.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

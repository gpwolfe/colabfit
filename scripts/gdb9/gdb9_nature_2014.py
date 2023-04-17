"""
author:gpwolfe

Data can be downloaded here:
https://doi.org/10.6084/m9.figshare.c.978904.v5

The direct download address:
https://springernature.figshare.com/articles/dataset/Data_for_6095_constitutional_isomers_of_C7H10O2/1057646?backTo=/collections/Quantum_chemistry_structures_and_properties_of_134_kilo_molecules/978904

Extract files to script directory
tar -xf dsgdb9nsd.xyz.tar.bz2 -C $project_dir/gdb9/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
free energy
potential energy

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

DATASET_FP = Path().cwd()

HEADER_RE = re.compile(
    r"gdb (?P<index>\d+)\s(?P<rotational_a>[-\d\.]+)\s"
    r"(?P<rotational_b>[-\d\.]+)\s(?P<rotational_c>[-\d\.]+)\s"
    r"(?P<dipole_moment>[-\d\.]+)\s(?P<isotropic_polarizability>[-\d\.]+)"
    r"\s(?P<homo>[-\d\.]+)\s(?P<lumo>[-\d\.]+)\s"
    r"(?P<homo_lumo_gap>[-\d\.]+)\s(?P<elect_spatial_extent>[-\d\.]+)"
    r"\s(?P<zpve>[-\d\.]+)\s(?P<internal_energy_0>[-\d\.]+)\s"
    r"(?P<internal_energy_298>[-\d\.]+)\s(?P<enthalpy>[-\d\.]+)\s"
    r"(?P<free_energy>[-\d\.]+)\s(?P<heat_capacity>[-\d\.]+)"
)

COORD_RE = re.compile(
    r"(?P<element>[a-zA-Z]{1,2})\s+(?P<x>\S+)\s+"
    r"(?P<y>\S+)\s+(?P<z>\S+)\s+(?P<mulliken>\S+)"
)


def properties_parser(re_match, line):
    groups = re_match.match(line)
    return groups.groupdict().items()


def xyz_parser(file_path, header_regex):
    file_path = Path(file_path)
    name = "gdb9_nature_2014"
    elem_coords = defaultdict(list)
    n_atoms = int()
    property_dict = defaultdict(float)
    with open(file_path, "r") as f:
        line_num = 0
        for line in f:
            if line_num == 0:
                n_atoms = int(line)
                line_num += 1
            elif line_num == 1:

                for k, v in properties_parser(header_regex, line):
                    if v == "-":
                        pass
                    else:
                        property_dict[k] = float(v)
                line_num += 1
            elif line_num < n_atoms + 2:
                if "*^" in line:
                    line = line.replace("*^", "e")
                elem_coord_items = properties_parser(COORD_RE, line)
                try:
                    for elem_coord, val in elem_coord_items:
                        elem_coords[elem_coord].append(val)
                except ValueError:
                    print("ValueError at {line} in {file_path}")
                line_num += 1
            else:
                return name, n_atoms, elem_coords, property_dict


def reader(file_path):
    name, n_atoms, elem_coords, properties = xyz_parser(file_path, HEADER_RE)
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
    # Load configurations
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N", "F"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    pds = [free_energy_pd, potential_energy_pd]
    for pd in pds:
        client.insert_property_definition(pd)
    metadata = {
        "software": {"value": "MOPAC, Gaussian 09"},
        "method": {"value": "DFT-B3LYP/"},
        "basis-set": {"value": "6-31G(2df,p)"},
        "heat-capacity": {
            "field": "heat_capacity",
            "units": "cal/(mol K)",
        },
        "dipole-moment": {"field": "dipole_moment", "units": "Debye"},
        "enthalpy": {"field": "enthalpy", "units": "Ha"},
        "homo-energy": {"field": "homo", "units": "Ha"},
        "lumo-energy": {"field": "lumo", "units": "Ha"},
        "homo-lumo-gap": {"field": "homo_lumo_gap", "units": "Ha"},
        "isotropic-polarizability": {
            "field": "isotropic_polarizability",
            "units": "Bohr^3",
        },
        "electronic-spatial-extent": {
            "field": "elect_spatial_extent",
            "units": "Bohr^2",
        },
        "zpve": {"field": "zpve", "units": "Ha"},
        "internal-energy-298K": {
            "field": "internal_energy_298",
            "units": "Ha",
        },
    }
    property_map = {
        "free-energy": [
            {
                "energy": {"field": "free_energy", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "potential-energy": [
            {
                "energy": {"field": "internal_energy_0", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
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
    # name = "GDB_9"
    # cs_ids = []
    # co_ids = client.get_data(
    #     "configurations",
    #     fields="hash",
    #     query={"hash": {"$in": all_co_ids}},
    #     ravel=True,
    # ).tolist()

    # print(
    #     "Configuration set ", f"({name}):".rjust(22), f"{len(co_ids)}".rjust(7)
    # )

    # cs_id = client.insert_configuration_set(
    #     co_ids,
    #     description="GDB-9 dataset, a subset of GDB-17",
    #     name=name,
    # )

    # cs_ids.append(cs_id)

    client.insert_dataset(
        # cs_ids,
        do_hashes=all_do_ids,
        name="GDB_9_nature_2014",
        authors=[
            "Raghunathan Ramakrishnan",
            "Pavlo O. Dral",
            "Matthias Rupp",
            "O. Anatole von Lilienfeld",
        ],
        links=[
            "https://doi.org/10.6084/m9.figshare.c.978904.v5",
            "https://doi.org/10.1038/sdata.2014.22",
        ],
        description="133,855 configurations of stable small organic molecules"
        " composed of CHONF. A subset of GDB-17, with calculations of energies"
        ", dipole moment, polarizability and enthalpy. Calculations performed"
        " at B3LYP/6-31G(2df,p) level of theory.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

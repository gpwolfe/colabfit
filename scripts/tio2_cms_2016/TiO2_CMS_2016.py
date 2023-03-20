"""
author:gpwolfe

Data can be downloaded from:
http://ann.atomistic.net/download/
Download link:
http://ann.atomistic.net/files/data-set-2016-TiO2.tar.bz2

Extract to project folder
tar -xf data-set-2016-TiO2.tar.bz2 -C  <project_dir>/scripts/tio2_cms_2016

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
forces

Other properties added to metadata:
total energy
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import re
import sys

DATASET_FP = Path().cwd()
DATASET = "TiO2_bulk_structures_2016"

SOFTWARE = "Quantum ESPRESSO"
METHODS = "DFT-PBE"

EN_RE = re.compile(r"^# total energy = (?P<energy>\-?\d+\.\d+) eV")
LATT_RE = re.compile(r"(\-?\d+\.\d+)\s+(\-?\d+\.\d+)\s+(\-?\d+\.\d+)")
COORD_F_RE = re.compile(
    r"(?P<element>\w+)\s+(?P<x>\-?\d+\.\d+)\s+"
    r"(?P<y>\-?\d+\.\d+)\s+(?P<z>\-?\d+\.\d+)"
    r"\s+(?P<f1>\-?\d+\.\d+)\s+(?P<f2>\-?\d+\.\d+)\s+(?P<f3>\-?\d+\.\d+)"
)


def reader(filepath):
    atoms = []
    energy = None
    elements = []
    lattice = []
    coords = []
    forces = []
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f.readlines()]
    l_no = 1
    for line in lines:
        if l_no == 1:
            energy = float(EN_RE.match(line).groups("energy")[0])
            l_no += 1
        elif 5 <= l_no <= 7:
            lattice.append(LATT_RE.match(line).groups())
            l_no += 1
        elif l_no >= 10:
            coord_force = COORD_F_RE.match(line)
            elements.append(coord_force.groups()[0])
            coords.append([float(x) for x in coord_force.groups()[1:4]])
            forces.append([float(x) for x in coord_force.groups()[4:]])
            l_no += 1
        else:
            l_no += 1
    lattice = np.array(lattice)
    lattice.reshape(lattice.shape[0] // 3, 3, 3)

    atoms = AtomicConfiguration(
        positions=coords, symbols=elements, cell=lattice
    )
    atoms.info["force"] = forces
    atoms.info["name"] = filepath.stem
    atoms.info["energy"] = energy
    return [atoms]


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Ti", "O"],
        reader=reader,
        glob_string="*.xsf",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "Ry/Bohr"},
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
    # cs_regexes = [
    #     [
    #         DATASET,
    #         ".*",
    #         f"All configurations from {DATASET} dataset",
    #     ]
    # ]

    # cs_ids = []

    # for i, (name, regex, desc) in enumerate(cs_regexes):
    #     co_ids = client.get_data(
    #         "configurations",
    #         fields="hash",
    #         query={
    #             "hash": {"$in": all_co_ids},
    #             "names": {"$regex": regex},
    #         },
    #         ravel=True,
    #     ).tolist()

    #     print(
    #         f"Configuration set {i}",
    #         f"({name}):".rjust(22),
    #         f"{len(co_ids)}".rjust(7),
    #     )
    #     if len(co_ids) > 0:
    #         cs_id = client.insert_configuration_set(
    #             co_ids, description=desc, name=name
    #         )

    #         cs_ids.append(cs_id)
    #     else:
    #         pass

    client.insert_dataset(
        pr_hashes=all_do_ids,
        name=DATASET,
        authors="N. Artrith, A. Urban",
        links=[
            "http://ann.atomistic.net/download/",
            "http://dx.doi.org/10.1016/j.commatsci.2015.11.047",
        ],
        description="Approximately 7,800 configurations of TiO2 used in the "
        "training and testing of the software package aenet. "
        "DFT calculations performed using Quantum ESPRESSO.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

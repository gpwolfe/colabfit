"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.5281/zenodo.7018572
Download links:
https://zenodo.org/record/7018573/files/nep.in?download=1
https://zenodo.org/record/7018573/files/nep.txt?download=1
https://zenodo.org/record/7018573/files/test.in?download=1
https://zenodo.org/record/7018573/files/train.in?download=1

Move to script folder

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
Potential energy
forces

Other properties added to metadata
----------------------------------
virial (6-vector)


File notes
----------
energy, virial?
-1047.855796 23.6717 9.39515 69.8057 -10.89163 8.26454 -2.84489
lattice
1.63123700e+01 0.00000000e+00 0.00000000e+00 0.00000000e+00 9.37147200e+00 \
    0.00000000e+00 0.00000000e+00 0.00000000e+00 3.43319720e+01
element coordinates force
C   1.32592800e+01  2.37400000e-01  1.14220900e+01 -5.76856000e-01 \
    -2.09074000e-01 -1.31760100e+00

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

DATASET_FP = Path("data")
DATASET = "NEP-qHPF"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"
LINKS = [
    "https://doi.org/10.5281/zenodo.7018572",
    "https://doi.org/10.1016/j.eml.2022.101929",
]
AUTHORS = "Penghua Ying"
DS_DESC = (
    "Approximately 275 configurations of training and testing data "
    "of monolayer quasi-hexagonal-phase fullerene (qHPF) membrane."
)
ELEMENTS = ["C"]
GLOB_STR = "*.in"

EN_V_RE = re.compile(
    r"^([-\.\de]+)\s([-\.\de]+)\s([-\.\de]+)\s"
    r"([-\.\de]+)\s([-\.\de]+)\s([-\.\de]+)\s([-\.\de]+)$"
)
CO_FO_RE = re.compile(r"^(\S)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$")
LATT_RE = re.compile(r"^(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)$")


def reader(filepath):
    counter = 1
    config_no = 0
    configs = []
    energy = None
    virial = []
    force = []
    coords = []
    cell = []

    with open(filepath, "r") as f:
        for line in f:
            if counter == 1:
                num_a = int(line.rstrip())
                counter += 1
            elif counter < num_a:
                counter += 1
                pass
            else:
                if line.startswith("C"):
                    match = CO_FO_RE.match(line)
                    coords.append([float(x) for x in match.groups()[1:4]])
                    force.append([float(x) for x in match.groups()[4:]])

                elif len(line.split()) == 7:
                    # check whether any data has been gathered yet
                    if energy:
                        config = AtomicConfiguration(
                            positions=coords,
                            symbols=["C" for x in coords],
                            cell=cell,
                        )
                        config.info["virial"] = virial
                        config.info["energy"] = energy
                        config.info["name"] = f"{filepath.stem}_{config_no}"
                        config.info["forces"] = force
                        config_no += 1
                        configs.append(config)
                        force = []
                        coords = []
                    match = EN_V_RE.match(line)
                    energy = float(match.groups()[0])
                    virial = [float(x) for x in match.groups()[1:]]
                elif len(line.split()) == 9:
                    match = LATT_RE.match(line)
                    cell = np.array([float(x) for x in match.groups()[:]])
                    cell = cell.reshape(3, 3)

        return configs


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
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
    }
    co_md_map = {
        "virial": {"field": "virial"},
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
                "forces": {"field": "forces", "units": "eV/A"},
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
    cs_regexes = [
        [
            DATASET,
            "test.*",
            f"All configurations from {DATASET} dataset",
        ],
        [
            DATASET,
            "train.*",
            f"All configurations from {DATASET} dataset",
        ],
    ]

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
            cs_id = client.insert_configuration_set(co_ids, description=desc, name=name)

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

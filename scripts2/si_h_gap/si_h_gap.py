"""
author:gpwolfe

Data can be downloaded from:

Download link:

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
virial

Other properties added to metadata
----------------------------------
energy sigma
virial sigma

File notes
----------
example info
'config_type': 'diamond111',
'energy_sigma': 0.07200000000000001,
'virial_sigma': 3.6,
'dft_energy': -13751.03625464858,
'dft_virial':
"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    cauchy_stress_pd,
    potential_energy_pd,
    atomic_forces_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("Si-H-GAP-main/structural_data")
DATASET = "Si-H-GAP"

SOFTWARE = "Quantum ESPRESSO"
METHODS = "DFT-PBE"
LINKS = [
    "https://github.com/dgunruh/Si-H-GAP",
    "https://doi.org/10.1103/PhysRevMaterials.6.065603",
]
AUTHORS = [
    "Davis Unruh",
    "Reza Vatan Meidanshahi",
    "Stephen M. Goodnick",
    "Gábor Csányi",
    "Gergely T. Zimányi",
]
DS_DESC = (
    "656 configurations of hydrogenated liquid and amorphous silicon, "
    "divided into reference, training and validation sets."
)
ELEMENTS = ["Si", "H"]
GLOB_STR = "*.xyz"


def reader(filepath):
    atoms = read(filepath, index=":", format="extxyz")
    for i, atom in enumerate(atoms):
        atom.info["name"] = f"{filepath.stem}_{atom.info['config_type']}_{i}"
        if atom.info.get("dft_virial") is not None:
            atom.info["stress"] = np.array(atom.info.get("dft_virial")).reshape(3, 3)
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
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
    }
    co_md_map = {
        "energy-sigma": {"field": "energy_sigma"},
        "virial-sigma": {"field": "virial_sigma"},
        "force-atom-sigma": {"field": "force_atom_sigma"}
        # "": {"field": ""}
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "dft_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "dft_force", "units": "eV/A"},
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
            f"{DATASET}-reference",
            "reference.*",
            f"Reference configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-training-alternate",
            ".*alternate.*",
            f"Training configurations from {DATASET} dataset with alternate "
            "regularization parameters",
        ],
        [
            f"{DATASET}-training-paper",
            ".*paper.*",
            f"Training configurations from {DATASET} dataset with regularization "
            "parameters shown in publication",
        ],
        [
            f"{DATASET}-validation",
            "validation.*",
            f"Validation configurations from {DATASET} dataset",
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

"""
author:gpwolfe

Data can be downloaded from:
https://github.com/FitSNAP/FitSNAP
Download link:
https://github.com/FitSNAP/FitSNAP/archive/refs/heads/master.zip

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
forces
potential energy

Other properties added to metadata
----------------------------------
spins

File notes
----------
This repository contains several datasets with info about associated
papers.

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import json
from pathlib import Path
import sys

DATASET_FP = Path("examples/Ta_Linear_JCP2014/")
DATASET = "FitSNAP-Ta-Linear-JCP-2014"

SOFTWARE = "VASP"
METHODS = "DFT(PBE)"
LINKS = [
    "https://github.com/FitSNAP",
    "https://doi.org/10.1016/j.jcp.2014.12.018",
]
AUTHORS = [
    "A.P. Thompson",
    "L.P. Swiler",
    "C.R. Trott",
    "S.M. Foiles",
    "G.J. Tucker",
]
DS_DESC = "363 configurations of Ta used in the training of\
 a Spectral Neighbor Analysis Potential (SNAP) interatomic potential ML\
 model with the goal of modeling the migration of screw dislocations in\
 tantalum metal under shear loading."
ELEMENTS = ["Ta"]
GLOB_STR = "*.json"


def reader(filepath):
    name = f"{filepath.parts[-2]}_{filepath.stem}"
    with open(filepath, "r") as f:
        data = []
        for line in f:
            if line.startswith("#") or not line.strip():
                pass
            else:
                data.append(line.strip())
        if len(data) > 1:
            data = "".join(data)
        else:
            data = data[0]
        data = json.loads(data)["Dataset"]["Data"][0]
        config = AtomicConfiguration(
            positions=data["Positions"],
            symbols=data["AtomTypes"],
            cell=data["Lattice"],
        )
        config.info["energy"] = data["Energy"]
        config.info["stress"] = data["Stress"]
        config.info["forces"] = data["Forces"]
        # if data.get("Spins"):
        #     config.info["spins"] = data["Spins"]
        # if data.get("Charges"):
        #     config.info["charges"] = data["Charges"]
        config.info["name"] = name
    configs = []
    configs.append(config)
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
    client = MongoDatabase(args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017")

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
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        # "charges": {"field": "charges"},
        # "spins": {"field": "spins"},
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
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "kB"},
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
    #         f"{DATASET}",
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
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
        # cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

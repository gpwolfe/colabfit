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
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import json
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/fitsnap/"
    "examples/Fe_Linear_NPJ2021/JSON"
)
DATASET_FP = Path().cwd().parent / "data/fitsnap/Fe_Linear_NPJ2021/JSON"
DATASET = "FitSNAP_Fe_NPJ_2021"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"
PUBLICATION = "https://github.com/FitSNAP"
DATA_LINK = "https://doi.org/10.1038/s41524-021-00617-2"
LINKS = [
    "https://github.com/FitSNAP",
    "https://doi.org/10.1038/s41524-021-00617-2",
]
AUTHORS = [
    "Svetoslav Nikolov",
    "Mitchell A. Wood",
    "Attila Cangi",
    "Jean-Bernard Maillet",
    "Mihai-Cosmin Marinica",
    "Aidan P. Thompson",
    "Michael P. Desjarlais",
    "Julien Tranchida",
]
DS_DESC = (
    "About 2,500 configurations of alpha-Fe used in the training and "
    "testing of a ML model with the goal of building magneto-elastic "
    "machine-learning interatomic potentials for large-scale spin-lattice "
    "dynamics simulations."
)
ELEMENTS = ["Fe"]
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
        if data.get("Spins"):
            config.info["spins"] = data["Spins"]
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
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
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
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
    }
    co_md_map = {
        "spins": {"field": "spins"},
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
                "forces": {"field": "forces", "units": "eV/angstrom"},
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
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

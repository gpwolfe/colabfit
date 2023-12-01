"""
author:gpwolfe

Data can be downloaded from:

Download link:

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
)
import json
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/combat")
DATASET_FP = Path().cwd().parent / "data/combat"
DATASET = "ComBat"

SOFTWARE = "Gaussian 16"
PUBLICATION = "https://doi.org/10.1038/s41598-022-20009-w"
DATA_LINK = "https://github.com/rashatwi/combat/"
LINKS = [
    "https://github.com/rashatwi/combat/",
    "https://doi.org/10.1038/s41598-022-20009-w",
]
AUTHORS = ["Rasha Atwi", "Matthew Bliss", "Maxim Makeev", "Nav Nidhi Rajput"]
DS_DESC = (
    "DFT-optimized geometries and properties for Li-S electrolytes. These "
    "make up the Computational Database for Li-S Batteries (ComBat), calculated using "
    "Gaussian 16 at the B3LYP/6-31+G* level of theory."
)
ELEMENTS = ["H", "S", "C", "O", "F", "Li", "P", "N", "Si"]
GLOB_STR = "*.json"


def reader(filepath):
    print(filepath)
    name = filepath.stem
    elements = []
    with open(filepath, "r") as f:
        data = json.load(f)
    basis = data["basis"]
    func = data["functional"]

    if "input" in data:
        data = data["output"]["output"]
        energy = data["final_energy"]

    else:
        energy = data.get("energy", data["molecule"].get("energy"))
    # dipole = data.get("dipole_moment")
    # polar = data.get("polarizability")
    sites = data["molecule"]["sites"]
    positions = [site["xyz"] for site in sites]
    elements = [site["species"][0]["element"] for site in sites]
    atoms = AtomicConfiguration(symbols=elements, positions=positions)
    atoms.info["energy"] = energy
    # atoms.info["dipole_moment"] = dipole
    # atoms.info["polarizability"] = polar
    atoms.info["basis"] = basis
    atoms.info["method"] = f"DFT-{func}"
    atoms.info["name"] = name
    configs = []
    configs.append(atoms)
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
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"field": "method"},
        "basis-set": {"field": "basis"},
        # "polarizability": {"field": "polarizability"},
        # "dipole-moment": {"field": "dipole_moment"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
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

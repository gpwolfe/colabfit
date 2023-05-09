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
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
)
import json
from pathlib import Path
import sys

DATASET_FP = Path().cwd()
DATASET = "MISPR"

SOFTWARE = "Gaussian 16"
LINKS = [
    "https://github.com/rashatwi/mispr-dataset",
    "https://doi.org/10.1038/s41598-022-20009-w",
]
AUTHORS = ["Rasha Atwi", "Matthew Bliss", "Maxim Makeev", "Nav Nidhi Rajput"]
DS_DESC = (
    "Example dataset for MISPR (Materials Informatics for Structure-Property "
    "Relationships) materials science simulation software, with DFT-calculated "
    "configuration properties for three different MISPR workflows: nuclear magnetic "
    "resonance (NMR) chemical shifts, electrostatic partial charges (ESP) and bond "
    "dissociation energies (BDE)."
)
ELEMENTS = ["H", "C", "O", "Cl", "S", "F", "N", "Si", "P"]
GLOB_STR = "*.json"


def reader(filepath):
    print(filepath)
    name = f"{filepath.parts[-3]}_{filepath.stem}"
    elements = []
    with open(filepath, "r") as f:
        data = json.load(f)
    basis = data["basis"]
    func = data["functional"]

    energy = data.get("energy")
    sites = data["molecule"]["sites"]
    positions = [site["xyz"] for site in sites]
    elements = [site["species"][0]["element"] for site in sites]
    atoms = AtomicConfiguration(symbols=elements, positions=positions)
    if energy:
        atoms.info["energy"] = energy
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
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"field": "method"},
        "basis-set": {"field": "basis"},
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
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            f"{DATASET}-BDE",
            "^bde_",
            f"Configurations from {DATASET} dataset produced by the MISPR bond "
            "dissociation energy (BDE) workflow",
        ],
        [
            f"{DATASET}-ESP",
            "^esp_",
            f"Configurations from {DATASET} dataset produced by the MISPR "
            "electrostatic partial charge (ESP) workflows",
        ],
        [
            f"{DATASET}-NMR",
            "^nmr_",
            f"Configurations from {DATASET} dataset produced by the MISPR nuclear "
            "magnetic resonance (NMR) workflow",
        ],
    ]
    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

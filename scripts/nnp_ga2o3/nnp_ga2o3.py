"""
author:gpwolfe

Data can be downloaded from:

Download link:


Extract to project folder
tar -xf example.tar -C  <project_dir>/data/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------

File notes
----------

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
import sys

DATASET_FP = Path("NNP_Ga2O3-master")
DATASET = "NNP-Ga2O3"

SOFTWARE = "CP2K"
METHODS = "DFT"
LINKS = [
    "https://github.com/RuiyangLi6/NNP_Ga2O3",
    "https://doi.org/10.1063/5.0025051",
]
AUTHORS = [
    "Ruiyang Li",
    "Zeyu Liu",
    "Andrew Rohskopf",
    "Kiarash Gordiz",
    "Asegun Henry",
    "Eungkyu Lee",
    "Tengfei Luo",
]
DS_DESC = (
    "9,200 configurations of beta-Ga2O3, including two configuration "
    "sets. One contains DFT data for 8400 configurations simulated between "
    "temperatures of 50K - 600K. The second contains configurations with 0K "
    "simulation temperature."
)
ELEMENTS = ["Ga", "O"]
GLOB_STR = "energy.npy"

ELEM_KEY = {0: "Ga", 1: "O"}


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    type_path = list(filepath.parents[1].glob("type.raw"))[0]

    with open(type_path, "r") as f:
        nums = f.read().rstrip().split(" ")
        props["symbols"] = [ELEM_KEY[int(num)] for num in nums]

    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = len(props["symbols"])
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    return props


def reader(filepath):
    props = assemble_props(filepath)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["forces"][i]
        c.info["energy"] = float(energy[i])
        c.info["name"] = f"NNP-Ga2O3_{filepath.parts[-3]}_{filepath.parts[-2]}_{i}"
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
        "basis-set": {"value": "GTH-TZVP"},
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
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            f"{DATASET}-with-0K",
            ".*with0K.*",
            f"All configurations from {DATASET} dataset, including those simulated "
            "between temperatures 0K - 600K",
        ],
        [
            f"{DATASET}-without-0K",
            ".*without0K.*",
            f"Configurations from {DATASET} dataset simulated between temperatures "
            "50K - 600K",
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

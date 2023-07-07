"""
author:gpwolfe

Data can be downloaded from:


Run: $ python3 <script_name>.py -i (or --ip) <database_ip> -d <database_name> \
    -p <number_of_processors>

Properties
----------
energy
forces
virial

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
    cauchy_stress_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("data/23-Single-Element-DNPs-main/Training_Data")
DATASET = "23-DNPs-RSCDD-2023"

LINKS = [
    "https://doi.org/10.1039/D3DD00046J",
    "https://github.com/saidigroup/23-Single-Element-DNPs",
]
AUTHORS = ["Christopher M. Andolina", "Wissam A. Saidi"]
DS_DESC = (
    "Minimalist, curated sets of DFT-calculated properties for many "
    "individual elements for the purpose of providing input to machine learning of "
    "atomic potentials. Each element set contains on average ~4000 structures with "
    "27 atoms per structure."
)
ELEMENTS = [
    "Ag",
    "Al",
    "Au",
    "Co",
    "Cu",
    "Ge",
    "I",
    "Kr",
    "Li",
    "Mg",
    "Mo",
    "Nb",
    "Ni",
    "Os",
    "Pb",
    # "Pd",
    # "Pt",
    # "Re",
    # "Sb",
    # "Sr",
    # "Ti",
    # "Zn",
    # "Zr",
]
GLOB_STR = "box.npy"
METHODS = "DFT-PBE"
SOFTWARE = "VASP"


def assemble_props(filepath: Path, element: str):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = props["force"].shape[1] // 3
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    virial = props.get("virial")
    if virial is not None:
        props["virial"] = virial.reshape(num_configs, 3, 3)
    props["symbols"] = [element for i in range(props["coord"].shape[1])]
    return props


def reader(filepath: Path, element: str):
    start_part = filepath.parts.index(element)
    name_parts = filepath.parts[start_part:-2]
    name = "_".join(name_parts)
    for part in name_parts:
        if part.isdigit():
            temp = int(part)
        if "_mp-" in part:
            mp_id = part.split("_")[1]

    props = assemble_props(filepath, element)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["forces"][i]
        virial = props.get("virial")
        if virial is not None:
            c.info["virial"] = virial[i]
        # if energy is not None:
        c.info["energy"] = float(energy[i])
        c.info["mp_id"] = mp_id
        c.info["name"] = f"{name}_{i}"
        if temp is not None:
            c.info["temp"] = temp
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
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        # "": {"field": ""}
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
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
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

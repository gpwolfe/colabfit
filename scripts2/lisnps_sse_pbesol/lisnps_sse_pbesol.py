"""
author:gpwolfe

Data can be downloaded from:
https://www.aissquare.com/datasets/detail?pageType=datasets&name=LiSnPS-SSE-PBEsol

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
energy
forces
virial

Other properties added to metadata
----------------------------------

File notes
----------
The last author name is Weinan E -- this is not a misspelling

This appears to be an exact copy of the dataset that uses the PBE
functional (from the same source, supposedly two datasets with different
training/DFT parameters)
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

DATASET_FP = Path().cwd()
DATASET = "LiSnPS-SSE-PBEsol"

SOFTWARE = "VASP, DP-GEN"
METHODS = "DFT-PBEsol"
LINKS = [
    "https://www.aissquare.com/datasets/detail?pageType=datasets&name=LiSnPS-SSE-PBEsol",
    "https://doi.org/10.1063/5.0041849",
]
AUTHORS = "J. Huang, L. Zhang, H. Wang, J. Zhao, J. Cheng, and W. E"
DS_DESC = "Approximately 2,700 configurations of Li10SnP2S12, based on crystal\
 structures from the Materials Project database, material ID mp-696123. One of\
 two LiSnPS datasets from this source. The other uses the PBE functional,\
 rather than the PBEsol functional."
ELEMENTS = ["Li", "Sn", "P", "S"]
GLOB_STR = "box.npy"

ELEM_MAP = {0: "Li", 1: "Sn", 2: "P", 3: "S"}


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    a_types = []
    with open(filepath.parents[1] / "type.raw", "r") as f:
        a_types = f.readlines()
        if len(a_types) == 1:
            a_types = [int(x.strip()) for x in a_types[0].split()]
        else:
            a_types = [int(x.strip()) for x in a_types]
    symbols = list(map(lambda x: ELEM_MAP.get(x), a_types))
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
    props["symbols"] = symbols
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
        virial = props.get("virial")
        if virial is not None:
            c.info["virial"] = virial[i]
        # if energy is not None:
        c.info["energy"] = float(energy[i])

        c.info["name"] = f"{filepath.parts[-3]}_{filepath.parts[-2]}_{i}"
    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

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

    client.insert_dataset(
        pr_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

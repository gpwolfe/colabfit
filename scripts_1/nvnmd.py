"""
author:gpwolfe

Data can be downloaded from:
https://github.com/LiuGroupHNU/nvnmd
Download link:
https://github.com/LiuGroupHNU/nvnmd/archive/refs/heads/master.zip
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
forces
potential energy

Other properties added to metadata
----------------------------------
dipole

File notes
----------
Not sure whether atom type mapping is correct.
For now, Ge=0, Te=1, but this might be reversed?
change ELEM_KEY if necessary
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/nvnmd/"
    "nvnmd-master/examples/data/GeTe"
)
DATASET_FP = Path().cwd().parent / "data/nvnmd"
DATASET = "NVNMD_GeTe"

SOFTWARE = "SIESTA"
METHODS = "DFT-GGA"

PUBLICATION = "https://doi.org/10.1038/s41524-022-00773-z"
DATA_LINK = "https://github.com/LiuGroupHNU/nvnmd"
OTHER_LINKS = ["https://doi.org/10.1109/LED.2020.2964779"]
LINKS = [
    "https://github.com/LiuGroupHNU/nvnmd",
    "https://doi.org/10.1038/s41524-022-00773-z",
    "https://doi.org/10.1109/LED.2020.2964779",
]
AUTHORS = [
    "Pinghui Mo",
    "Chang Li",
    "Dan Zhao",
    "Yujia Zhang",
    "Mengchao Shi",
    "Junhua Li",
    "Jie Liu",
]
DS_DESC = (
    "Approximately 5,000 configurations of GeTe used in training of a "
    "non-von Neumann multiplication-less DNN model."
)
ELEMENTS = ["Ge", "Te"]
GLOB_STR = "box.npy"

ELEM_KEY = {0: "Ge", 1: "Te"}


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    type_path = list(filepath.parents[1].glob("type.raw"))[0]

    with open(type_path, "r") as f:
        nums = f.read().strip().split("\n")
        props["symbols"] = [ELEM_KEY[int(num)] for num in nums]

    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = props["force"].shape[1] // 3
    props["force"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["dipole"] = props["dipole"].reshape(num_configs, num_atoms, 3)
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
        c.info["forces"] = props["force"][i]
        c.info["dipole"] = props["dipole"][i]
        c.info["energy"] = float(energy[i])
        c.info["name"] = f"{filepath.parts[-3]}_{filepath.parts[-2]}_{i}"
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

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {"encut": {"value": 100, "units": "rydberg"}},
    }
    co_md_map = {
        "dipole": {"field": "dipole"},
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
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

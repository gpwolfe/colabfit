"""
author:gpwolfe

Data can be downloaded from:
https://github.com/UncertaintyQuantification/AFF/tree/master

Extract to project folder
unzip AFF-master.zip "*.npz" -d <project_dir>

Remove duplicate data (if desired)
rm aspirin_rearrange.npz

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
None

File notes
----------
with keys: ['E', 'F', 'R', 'z', 'name', 'theory', 'md5', 'type']
alkane.npz   aspirin_ccsd-train.npz
aspirin_new_musen.npz   glucose_alpha.npz   uracil_dft.npz
with keys: ['E', 'F', 'R', 'z']
aspirin_rearrange.npz
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from collections import defaultdict
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("AFF-master/dataset/")

DATASET = "AFF_JCP_2022"
METHODS = "DFT-PBE-TS-vdW"
BASIS_SET = "6-31G(d,p)"
SOFTWARE = "Q-Chem"
LINKS = [
    "https://doi.org/10.1063/5.0088017",
    "https://github.com/UncertaintyQuantification/AFF/tree/master",
]
AUTHORS = [
    "Hao Li",
    "Musen Zhou",
    "Jessalyn Sebastian",
    "Jianzhong Wu",
    "Mengyang Gu",
]
DS_DESC = """Approximately 145,000 configurations of alkane, \
aspirin, alpha-glucose and uracil, partly taken from the \
MD-17 dataset, used in training an 'Atomic Neural Net' \
model."""


def read_npz(filepath):
    data = defaultdict(list)
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
    return data


def reader(filepath):
    configs = []
    data = read_npz(filepath)
    # The only file without a name attribute should be "aspirin_rearrange.npz"
    name = filepath.stem.split("_")[0]
    if name == "glucose":
        name = "alpha-glucose"
    print(name)
    for i, coord in enumerate(data["R"]):
        config = AtomicConfiguration(positions=coord, numbers=data["z"])
        config.info["name"] = name
        config.info["energy"] = float(data["E"][i])
        config.info["forces"] = data["F"][i]
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
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "basis-set": {"value": BASIS_SET},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/mol/A"},
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
            f"{DATASET}-aspirin",
            "aspirin",
            f"Aspirin configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-uracil",
            "uracil",
            f"Uracil configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-alkane",
            "alkane",
            f"Alkane configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-alpha-glucose",
            "alpha-glucose",
            f"alpha-glucose configurations from {DATASET} dataset",
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

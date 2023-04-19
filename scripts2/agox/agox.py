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

There are two kinds of files involved:
sqlite database files contain generated data from AGOX software
.traj files contain relaxation trajectories

"""
from argparse import ArgumentParser
from ase.io import read
from ase.calculators.calculator import PropertyNotImplementedError
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import re
import sqlite3
import sys

DATASET_FP = Path().cwd()
DATASET = "AGOX"

SOFTWARE = "AGOX-GPAW"
METHODS = "DFT-PBE"
LINKS = [
    "https://gitlab.com/agox/agox_data",
    "https://doi.org/10.1063/5.0121748",
]
AUTHORS = [
    "Nikolaj Rønne",
    "Mads-Peter V. Christiansen",
    "Andreas Møller Slavensky",
    "Zeyuan Tang",
    "Florian Brix",
    "Mikkel Elkjær Pedersen",
    "Malthe Kjær Bisbo",
    "Bjørk Hammer",
]
DS_DESC = "Dataset with generated configurations and some relaxation\
 trajectories created with AGOX (Atomistic Global Optimization X)\
 software. Most DFT calculations use GPAW code. The exception is the\
 cobalt-doped boron (CoB-) dataset, which uses ORCA code.\
    "
ELEMENTS = ["C", "H", "O", "N", "Ru", "Pt", "Au", "Ag", "Sn"]
GLOB_STR = "*.db"
GLOB_STR2 = "*.traj"

RE = re.compile(r"")

NAMES = {
    "C4NH5",
    "C5NH5",
    "C24",
    "C30",
    "CoB",
    "Pt14_Au",
    "SnO2",
    "Ru4N3C4_graphene",
    "AgxOy_2layer_C",
}


def decode_reshape(vals):
    arr = np.frombuffer(vals, dtype=np.float64)
    arr = arr.reshape(arr.shape[0] // 3, 3)
    return arr


def namer(filepath: Path):
    parts = filepath.parts
    for name in NAMES:
        if name in parts[-6:]:
            if filepath.suffix == ".db":
                return f"{name}_generated_{filepath.stem}"
            elif filepath.suffix == ".traj":
                return f"{name}_trajectory_{filepath.stem}"
            else:
                return "No name found"


def read_db(filepath):
    print(filepath)
    configs = []
    name = namer(filepath)
    if name == "CoB":
        software = "AGOX-ORCA"
    else:
        software = SOFTWARE
    con = sqlite3.connect(filepath)
    cur = con.execute("SELECT * FROM structures")
    for struct in cur.fetchall():
        index = struct[0]
        ctime = struct[1]
        coords = decode_reshape(struct[2])
        energy = struct[3]
        types = [int(a_type) for a_type in np.frombuffer(struct[4])]
        cell = decode_reshape(struct[5])
        forces = decode_reshape(struct[6])
        if len(struct) > 7:
            pbc = [int(a_type) for a_type in np.frombuffer(struct[7])]
            template_indices = [
                int(a_type) for a_type in np.frombuffer(struct[8])
            ]
            config = AtomicConfiguration(
                numbers=types, positions=coords, pbc=pbc, cell=cell
            )
        else:
            template_indices = None
            config = AtomicConfiguration(
                numbers=types, positions=coords, cell=cell
            )
        config.info["energy"] = energy
        config.info["forces"] = forces
        config.info["ctime"] = ctime
        if template_indices:
            config.info["template_indices"] = template_indices
        config.info["software"] = software
        config.info["name"] = f"{name}_{index}"
        configs.append(config)
    return configs


def read_traj(filepath):
    name = namer(filepath)
    configs = []
    data = read(filepath, index=":")
    for i, config in enumerate(data):
        try:
            forces = config.get_forces()
            config.info["forces"] = forces.reshape(forces.shape[0] // 3, 3)
        except (PropertyNotImplementedError, ValueError):
            pass
        config.info["energy"] = config.get_potential_energy()
        config.info["software"] = SOFTWARE
        config.info["name"] = f"{name}_{i}"
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

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"field": "software"},
        "method": {"value": METHODS},
        "ctime": {"field": "ctime"},
        "template_indices": {"field": "template_indices"},
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
            },
        ],
    }

    globs = list(set([db.parent for db in DATASET_FP.rglob(GLOB_STR)]))
    configurations = load_data(
        file_path=globs[0],
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=read_db,
        glob_string=GLOB_STR,
        generator=False,
    )
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )
    for gl in globs:
        configurations = load_data(
            file_path=gl,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=read_db,
            glob_string=GLOB_STR,
            generator=False,
        )

        ids.extend(
            client.insert_data(
                configurations,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=read_traj,
        glob_string=GLOB_STR2,
        generator=False,
    )

    ids.extend(
        list(
            client.insert_data(
                configurations,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = []
    for name in NAMES:
        cs_regexes.append(
            [
                f"{DATASET}_{name}_generated",
                f"{name}_generated",
                f"Configurations of {name} from {DATASET} dataset generated by AGOX ML-assisted parallel tempering basin hopping (PT-BH) structure optimation algorithm",
            ]
        )
    for name in ["Ru4N3C4_graphene", "C5NH5"]:
        cs_regexes.append(
            [
                f"{DATASET}_{name}_trajectory",
                f"{name}_trajectory",
                f"Relaxation trajectories for {name} from {DATASET} dataset",
            ]
        )

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
        # cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

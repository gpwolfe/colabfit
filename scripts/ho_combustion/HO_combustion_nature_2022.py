"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.6084/m9.figshare.19601689.v3
Download link:
https://figshare.com/ndownloader/files/36445941

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>
"""
# npz files
# *_aimd.npz = ab initio mol. dyn.
# *_irc.npz = intrinsic reaction coordinate
# *_nm.npz = normal mode displacement
# keys from file:
# R = coordinates
# Z = atom number
# N = number of atoms >> the array size (for force) is always [x, 6, 3],
#       as if there were 6 atoms, but the forces for the missing atoms will
#       be set to float(0). Therefore use [x, N, 3]
# E = reference potential energy (per publication)
# F = forces
# RXN = reaction number?
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

DATASET_FP = Path("H2COMBUSTION_DATA-main-2")

METHODS = "DFT(ωB97X-V)"
SOFTWARE = "Q-CHEM"
DATASET = "H_nature_2022"


def read_npz(filepath):
    data = defaultdict(list)
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
        data["name"] = filepath.stem
    return data


def reader(filepath):
    data = read_npz(filepath)
    name = data["name"]
    rxn = data["RXN"][0]
    atoms = []
    for i, coords in enumerate(data["R"]):
        N = int(data["N"][0])
        coords = coords[:N]
        atom = AtomicConfiguration(positions=coords, numbers=data["Z"][i][:N])
        atom.info["name"] = name
        atom.info["forces"] = data["F"][i][:N]
        atom.info["energy"] = data["E"][i][0]
        atom.info["reaction-number"] = rxn
        atoms.append(atom)
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "reaction-number": {"field": "reaction-number"},
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
            f"IRC_{DATASET}",
            ".*irc",
            f"Intrinsic reaction coordinate configurations from {DATASET}",
        ],
        [
            f"AIMD_{DATASET}",
            ".*aimd",
            f"ab initio configurations from {DATASET}",
        ],
        [
            f"NM_{DATASET}",
            ".*nm",
            f"Normal mode displacement configurations from {DATASET}",
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
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        cs_ids=cs_ids,
        pr_hashes=all_do_ids,
        name=DATASET,
        authors=[
            "Xingyi Guan",
            "Akshaya Das",
            "Christopher J. Stein",
            "Farnaz Heidar-Zadeh",
            "Luke Bertels",
            "Meili Liu",
            "Mojtaba Haghighatlari",
            "Jie Li",
            "Oufan Zhang",
            "Hongxia Hao",
            "Itai Leven",
            "Martin Head-Gordon",
            "Teresa Head-Gordon",
        ],
        links=[
            "https://doi.org/10.6084/m9.figshare.19601689.v3",
            "https://doi.org/10.1038/s41597-022-01330-5",
        ],
        description="Over 300,000 configurations in an expanded "
        "dataset of 19 hydrogen combustion reaction "
        "channels. Intrinsic reaction coordinate calculations (IRC) are "
        "combined with ab initio simulations (AIMD) and normal mode "
        "displacement (NM) calculations.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

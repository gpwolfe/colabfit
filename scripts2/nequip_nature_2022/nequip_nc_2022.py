"""
author:gpwolfe

Data can be downloaded from:

Download link:

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------
None

File notes
----------
Other datasets for this publication were taken from already-imported datasets:
MD17, rMD17, etc.
"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path().cwd()
DATASET = "NequIP-NC-2022"

SOFT_METH = {
    "lipo-quench": ("DFT(PBE)", "VASP"),
    "lips": ("DFT(PBE)", "VASP"),
    "fcu": ("DFT", "CP2K"),
}

LINKS = [
    "https://doi.org/10.24435/materialscloud:s0-5n",
    "https://doi.org/10.1038/s41467-022-29939-5",
]
AUTHORS = [
    "Simon Batzner",
    "Albert Musaelian",
    "Lixin Sun",
    "Mario Geiger",
    "Jonathan P. Mailoa",
    "Mordechai Kornbluth",
    "Nicola Molinari",
    "Tess E. Smidt",
    "Boris Kozinsky",
]
DS_DESC = "Approximately 57,000 configurations from the evaluation datasets\
 for NequIP graph neural network model for\
 interatomic potentials. Trajectories have been taken from LIPS, LIPO glass\
 melt-quench simulation, and formate decomposition on Cu datasets. "
ELEMENTS = ["H", "C", "Cu", "Li", "P", "S", "O"]
GLOB_STR = "*.xyz"


def reader(filepath):
    name = filepath.stem
    atoms = read(filepath, index=":", format="extxyz")
    for i, atom in enumerate(atoms):
        atom.info["energy"] = atom.get_potential_energy()
        atom.info["forces"] = atom.get_forces()
        atom.info["name"] = f"{name}_{i}"
        atom.info["method"] = SOFT_METH[name][0]
        atom.info["software"] = SOFT_METH[name][1]

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
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"field": "software"},
        "method": {"field": "method"},
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
            f"{DATASET}-LIPS",
            "lips*",
            f"Lithium Thiophosphate (Li6.75P3S11) configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-LIPO-quench",
            "lipo-quench*",
            f"Lithium Phosphate amorphous glass (Li4P2O7) configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-Cu-formate",
            "fcu*",
            f"Cu-formate configurations, Cu <110> undergoing dehydrogenation decomposition, from {DATASET} dataset",
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
        pr_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

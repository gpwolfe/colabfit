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
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/nequip_nature_2022"
)
DATASET_FP = Path().cwd().parent / "data/nequip_nature_2022"
DATASET = "NequIP_NC_2022"

SOFT_METH = {
    "lipo-quench": ("DFT-PBE", "VASP"),
    "lips": ("DFT-PBE", "VASP"),
    "fcu": ("DFT", "CP2K"),
}

PUBLICATION = "https://doi.org/10.1038/s41467-022-29939-5"
DATA_LINK = "https://doi.org/10.24435/materialscloud:s0-5n"
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
DS_DESC = (
    "Approximately 57,000 configurations from the evaluation datasets "
    "for NequIP graph neural network model for "
    "interatomic potentials. Trajectories have been taken from LIPS, LIPO glass "
    "melt-quench simulation, and formate decomposition on Cu datasets. "
)
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
        "software": {"field": "software"},
        "method": {"field": "method"},
        "input": {
            "value": {
                "encut": {"value": 520, "units": "eV"},
                "kspacing": {"value": 0.25, "units": "Ang^-1"},
                "sigma": 0.1,
                "ismear": 0,
            }
        },
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
    cs_regexes = [
        [
            f"{DATASET}_LIPS",
            "lips*",
            f"Lithium Thiophosphate (Li6.75P3S11) configurations from {DATASET} "
            "dataset",
        ],
        [
            f"{DATASET}_LIPO_quench",
            "lipo-quench*",
            "Lithium Phosphate amorphous glass (Li4P2O7) configurations from "
            f"{DATASET} dataset",
        ],
        [
            f"{DATASET}_Cu_formate",
            "fcu*",
            "Cu-formate configurations, Cu <110> undergoing dehydrogenation "
            f"decomposition, from {DATASET} dataset",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=False,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

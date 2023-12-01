"""
author:gpwolfe

Data can be downloaded from:
https://github.com/argonne-lcf/active-learning-md

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
virial stress
forces

Other properties added to metadata
----------------------------------

File notes
----------
free energy is reported, but values are exactly the same as 'energy'

"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/hfo2_gap/data"
)
DATASET = "HfO2_NPJ_2020"

SOFTWARE = "VASP 5.4.4"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1038/s41524-020-00367-7"
DATA_LINK = "https://github.com/argonne-lcf/active-learning-md"
LINKS = [
    "https://github.com/argonne-lcf/active-learning-md",
    "https://doi.org/10.1038/s41524-020-00367-7",
]
AUTHORS = [
    "Ganesh Sivaraman",
    "Anand Narayanan Krishnamoorthy",
    "Matthias Baur",
    "Christian Holm",
    "Marius Stan",
    "Gábor Csányi",
    "Chris Benmore",
    "Álvaro Vázquez-Mayagoitia",
]
DS_DESC = "6000 configurations of liquid and amorphous HfO2 generated for use\
 with an active learning ML model."
ELEMENTS = ["Hf", "O"]
GLOB_STR = "*.extxyz"


def reader(filepath):
    atoms = read(filepath, index=":")
    for i, atom in enumerate(atoms):
        atom.info["name"] = f"{filepath.stem}_{i}"
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
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {"encut": {"value": 520, "units": "eV"}, "kpoints": "2 x 2 x 2"},
        },
    }
    # co_md_map = {
    #     "free-energy": {"field": "free_energy"}
    #     # "": {"field": ""}
    # }
    property_map = {
        # "free-energy": [
        #     {
        #         "energy": {"field": "free_energy", "units": "eV"},
        #         "per-atom": {"value": False, "units": None},
        #         "_metadata": metadata,
        #     }
        # ],
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
                "stress": {"field": "stress", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            # co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

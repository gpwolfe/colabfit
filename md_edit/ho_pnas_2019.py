"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2018.0020/v1

The only file necessary is:
https://archive.materialscloud.org/record/file?filename=training-set.zip&record_id=71

File address:

Unzip file to a new parent directory before running script.
mkdir <project_dir>/scripts/ho_pnas_2019
unzip training-set.zip "*.xyz"  -d <project_directory>/scripts/ho_pnas_2019/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""
from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/ho_pnas_2019/training-set"
)
DS_NAME = "HO_PNAS_2019"
DATA_LINK = "https://archive.materialscloud.org/record/2018.0020/v1"
PUBLICATION = "https://doi.org/10.1073/pnas.1815117116"
LINKS = [
    "https://archive.materialscloud.org/record/2018.0020/v1",
    "https://doi.org/10.1073/pnas.1815117116",
]
AUTHORS = [
    "Bingqing Cheng",
    "Edgar A. Engel",
    "Jörg Behler",
    "Christoph Dellago",
    "Michele Ceriotti",
]
DS_DESC = (
    "1590 configurations of H2O/water "
    "with total energy and forces calculated using "
    "a hybrid approach at DFT/revPBE0-D3 level of theory."
)


def reader(file_path):
    file_name = file_path.stem
    atoms = ase.io.read(file_path, index=":")
    for atom in atoms:
        atom.info["name"] = file_name
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
    ds_id = generate_ds_id()
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    # Load from colabfit's definitions
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "CP2K"},
        "method": {"value": "DFT-revPBE0+D3"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "TotEnergy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            ds_id=ds_id,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

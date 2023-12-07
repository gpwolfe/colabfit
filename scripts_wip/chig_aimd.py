"""
author:

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
NPROCS may need to be edited manually, as this is passed to the reader function
that does not allow multiple arguments in this framework

From the README:
In each npz, the index is shown as lists. Item 0 is the index of the corresponding
subfolder of "Force" and "Coordinates" directories, item 1 is index of the anchor
and item 2 is the index of the conformation that was simulated starting from the anchor.

coordinates file header:
# ORCA AIMD Position Step 26, t=26.00 fs, E_Pot=-4511.60955077 Hartree, Unit is Angstrom

forces file header:
# ORCA AIMD Force Step 26, t=26.00 fs, E_Pot=-4511.48191313 Hartree,\
    Unit is Hartree/Angstrom

"""
from argparse import ArgumentParser
from itertools import chain
from multiprocessing import Pool
from pathlib import Path
import sys

from ase.io import read
import numpy as np
from tqdm import tqdm

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

NPROCS = 16


DATASET_FP = Path("data/AIMD-Chig/")
DATASET_NAME = "Chig-AIMD"

SOFTWARE = "ORCA 4.2.1"
METHODS = "DFT-M06-2X"
PUBLICATION = "https://doi.org/10.1038/s41597-023-02465-9"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.22786730.v4"
LINKS = [
    "https://doi.org/10.6084/m9.figshare.22786730.v4",
    "https://doi.org/10.1038/s41597-023-02465-9",
]
AUTHORS = ["Tong Wang", "Xinheng He", "Mingyu Li", "Bin Shao", "Tie-Yan Liu"]
DATASET_DESC = (
    "This dataset covers the conformational space of chignolin with "
    "DFT-level precision. We sequentially applied replica exchange molecular "
    "dynamics (REMD), conventional MD, and ab initio MD (AIMD) simulations on a "
    "10 amino acid protein, Chignolin, and finally collected 2 "
    "million biomolecule structures with quantum level energy and force records."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "basis-set": {"value": "6-31G*"},
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "E_pot", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "hartree/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
}
force_dir = DATASET_FP / "Forces/Forces"
geom_dir = DATASET_FP / "Geometry optimization"
coord_dir = DATASET_FP / "Coordinates/Coordinates"


def get_split(data):
    configs = []
    for i, config_row in tqdm(enumerate(data)):
        force_f = force_dir / str(config_row[0]) / f"Force_{config_row[1]}.xyz"
        coord_f = coord_dir / str(config_row[0]) / f"Coordinates_{config_row[1]}.xyz"
        config = read(coord_f, index=config_row[2])
        config.info["forces"] = read(force_f, index=config_row[2]).positions
        config.info[
            "name"
        ] = f"dir{config_row[0]}_file{config_row[1]}_ix{config_row[2]}"
        configs.append(config)
    return configs


def train_reader(fp):
    pool = Pool(NPROCS)
    split_data = np.load(fp)["train_id"]
    chunks = np.array_split(split_data, NPROCS, axis=0)
    configs = chain.from_iterable(pool.map(get_split, chunks))
    for i, config in tqdm(enumerate(configs)):
        name = f"chig_aimd_train_{config.info['name']}_{i}"
        config.info["name"] = name
        config.info["labels"] = ["train", f"anchor_{fp.parts[-2]}"]

        yield config


def test_reader(fp):
    pool = Pool(NPROCS)
    split_data = np.load(fp)["test_id"]
    chunks = np.array_split(split_data, NPROCS, axis=0)
    configs = chain.from_iterable(pool.map(get_split, chunks))
    for i, config in tqdm(enumerate(configs)):
        config.info["name"] = f"chig_aimd_test_{i}"
        config.info["labels"] = ["test"]
        yield config


def val_reader(fp):
    pool = Pool(NPROCS)
    split_data = np.load(fp)["val_id"]
    chunks = np.array_split(split_data, NPROCS, axis=0)
    configs = chain.from_iterable(pool.map(get_split, chunks))
    for i, config in tqdm(enumerate(configs)):
        config.info["name"] = f"chig_aimd_val_{i}"
        config.info["labels"] = ["validation"]
        yield config


DSS = (
    # random splits
    (
        "Chig-AIMD_random_train",
        "random.npz",
        train_reader,
        "Training configurations from the 'random' split of Chig-AIMD. ",
    ),
    (
        "Chig-AIMD_random_test",
        "random.npz",
        test_reader,
        "Test configurations from the 'random' split of Chig-AIMD. ",
    ),
    (
        "Chig-AIMD_random_val",
        "random.npz",
        val_reader,
        "Validation configurations from the 'random' split of Chig-AIMD. ",
    ),
    # scaffold splits
    (
        "Chig-AIMD_scaffold_train",
        "scaffold.npz",
        train_reader,
        "Training configurations from the 'scaffold' split of Chig-AIMD. ",
    ),
    (
        "Chig-AIMD_scaffold_test",
        "scaffold.npz",
        test_reader,
        "Test configurations from the 'scaffold' split of Chig-AIMD. ",
    ),
    (
        "Chig-AIMD_scaffold_val",
        "scaffold.npz",
        val_reader,
        "Validation configurations from the 'scaffold' split of Chig-AIMD. ",
    ),
)


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

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    for ds_name, ds_glob, ds_reader, ds_desc in DSS:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DATASET_FP / "Split",
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=ds_reader,
            glob_string=ds_glob,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=LINKS,
            description=ds_desc + DATASET_DESC,
            verbose=True,
            # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        )


if __name__ == "__main__":
    main(sys.argv[1:])

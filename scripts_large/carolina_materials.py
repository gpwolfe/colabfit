"""
author: Gregory Wolfe

File notes
----------
Files have been renamed to data.mdb and lock.mdb to conform to lmdb

row keys
"_symmetry_space_group_name_H-M",
"_cell_length_a",
"_cell_length_b",
"_cell_length_c",
"_cell_angle_alpha",
"_cell_angle_beta",
"_cell_angle_gamma",
"_symmetry_Int_Tables_number",
"_chemical_formula_structural",
"_chemical_formula_sum",
"_cell_volume",
"_cell_formula_units_Z",
"symmetry_dict",
"atomic_numbers",
"cart_coords",
"energy",
"formula_pretty"
"""

from argparse import ArgumentParser
import json
import lmdb
from pathlib import Path
import pickle
import sys

from ase.atoms import Atoms

# from colabfit.tools.converters import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id

# from colabfit.tools.property_definitions import potential_energy_pd, free_energy_pd

# Kubernetes pod location
DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets_2.0/carolina_materials"
)
DATASET_FP = Path("data/carolina_matdb")  # local
DATASET_NAME = "Carolina_Materials"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1002/advs.202100566"
DATA_LINK = "https://zenodo.org/records/8381476"
OTHER_LINKS = ["http://www.carolinamatdb.org/", "https://github.com/IntelLabs/matsciml"]
LINKS = [
    "https://zenodo.org/records/8381476",
    "https://doi.org/10.1002/advs.202100566",
    "http://www.carolinamatdb.org/",
    "https://github.com/IntelLabs/matsciml",
]
AUTHORS = [
    "Yong Zhao",
    "Mohammed Al-Fahdi",
    "Ming Hu",
    "Edirisuriya M. D. Siriwardane",
    "Yuqi Song",
    "Alireza Nasiri",
    "Jianjun Hu",
]
DATASET_DESC = (
    "Carolina Materials contains structures used to train several machine "
    "learning models for the efficient generation of hypothetical inorganic materials. "
    "The database is built using structures from OQMD, Materials Project and "
    "ICSD, as well as ML generated structures validated by DFT. "
)
ELEMENTS = None
GLOB_STR = "data.mdb"
# LICENSE = "https://creativecommons.org/licenses/by/4.0/"
LICENSE = "Creative Commons Attribution 4.0 International License"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "input": {"value": {"IBRION": 6, "NFREE": 4}},
}

PROPERTY_MAP = {
    "formation-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}
CO_MD = {
    key: {"field": key}
    for key in [
        "_symmetry_space_group_name_H-M",
        "_symmetry_Int_Tables_number",
        "_chemical_formula_structural",
        "_chemical_formula_sum",
        "_cell_volume",
        "_cell_formula_units_Z",
        "symmetry_dict",
        "formula_pretty",
    ]
}
with open("formation_energy.json", "r") as f:
    formation_energy_pd = json.load(f)


def load_row(txn, row):
    try:
        data = pickle.loads(txn.get(f"{row}".encode("ascii")))
        return data
    except TypeError:
        return False


def config_from_row(row: dict, row_num: int):
    coords = row.pop("cart_coords")
    a_num = row.pop("atomic_numbers")
    cell = [
        row.pop(x)
        for x in [
            "_cell_length_a",
            "_cell_length_b",
            "_cell_length_c",
            "_cell_angle_alpha",
            "_cell_angle_beta",
            "_cell_angle_gamma",
        ]
    ]
    config = Atoms(scaled_positions=coords, numbers=a_num, cell=cell)
    symmetry_dict = {str(key): val for key, val in row.pop("symmetry_dict").items()}
    for key in symmetry_dict:
        key = str(key)
    config.info = row
    config.info["symmetry_dict"] = symmetry_dict
    config.info["name"] = f"carolina_materials_{row_num}"
    # print(config.info)
    return config


def reader(fp: Path):
    parent = fp.parent
    env = lmdb.open(str(parent))
    txn = env.begin()
    row_num = 0
    while True:
        row = load_row(txn, row_num)
        if row is False:
            env.close()
            break
        yield config_from_row(row, row_num)
        row_num += 1


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
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=True,
    )
    client.insert_property_definition(formation_energy_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_MD,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DATASET_DESC,
        data_license=LICENSE,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

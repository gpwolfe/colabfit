"""
author:gpwolfe

Data can be downloaded from:
https://pubs.acs.org/doi/10.1021/acs.jctc.1c00647?goto=supporting-info
Download link:
https://pubs.acs.org/doi/suppl/10.1021/acs.jctc.1c00647/suppl_file/ct1c00647_si_002.zip

Extract to project folder
unzip ct1c00647_si_002.zip -d $project_dir/scripts/3bpa

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------
dihedrals

File notes
----------

"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import re
import sys

DATASET_FP = Path().cwd()
DATASET = "3BPA"

SOFTWARE = "ORCA"
METHODS = "DFT-ωB97X"
LINKS = ["https://doi.org/10.1021/acs.jctc.1c00647"]
AUTHORS = [
    "Dávid Péter Kovács",
    "Cas van der Oord",
    "Jiri Kucera",
    "Alice E. A. Allen",
    "Daniel J. Cole",
    "Christoph Ortner",
    "Gábor Csányi",
]
DS_DESC = "Approximately 14,000 configurations from the training sets and\
 test sets used to showcase the performance of linear atomic cluster expansion\
 (ACE) force fields in a machine learning model to predict the potential \
 energy surfaces of organic molecules."
ELEMENTS = ["C", "H", "O", "N"]
GLOB_STR = "*.xyz"

RE = re.compile(r"")


def reader(filepath):
    name = filepath.stem
    configs = read(filepath, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
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
        "basis_set": "6-31G(d)",
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
    co_md_map = {"dihedrals": {"field": "dihedrals"}}
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            co_md_map=co_md_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            f"{DATASET}-isolated-atoms",
            "iso_atoms*",
            f"Reference C, H, O, and N atoms from {DATASET} dataset",
        ],
        [
            f"{DATASET}-test-300K",
            "test_300K*",
            f"Test configurations from {DATASET} dataset;"
            " MD simulation performed at 300K",
        ],
        [
            f"{DATASET}-test-600K",
            "test_600K*",
            f"Test configurations from {DATASET} dataset; "
            "MD simulation performed at 600K",
        ],
        [
            f"{DATASET}-test-1200K",
            "test_1200K*",
            f"Test configurations from {DATASET} dataset; MD "
            "simulation performed at 1200K",
        ],
        [
            f"{DATASET}-train-300K",
            "train_300K*",
            f"Training configurations from {DATASET} dataset; "
            "MD simulation performed at 300K",
        ],
        [
            f"{DATASET}-train-mixed",
            "train_mixedT*",
            f"Training configurations from {DATASET} dataset; "
            "mixed set with MD simulation performed at 300K, 600K and 1200K",
        ],
        [
            f"{DATASET}-test-300K",
            "test_300K*",
            f"Test configurations from {DATASET} dataset; "
            "MD simulation performed at 300K",
        ],
        [
            f"{DATASET}-test-dih-beta120",
            "test_dih_beta120*",
            f"Test configurations from {DATASET} dataset; "
            "fixed value for dihedral beta in alpha-gamma plane: 120 degrees",
        ],
        [
            f"{DATASET}-test-dih-beta150",
            "test_dih_beta150*",
            f"Test configurations from {DATASET} dataset; "
            "fixed value for dihedral beta in alpha-gamma plane: 150 degrees",
        ],
        [
            f"{DATASET}-test-dih-beta180",
            "test_dih_beta180*",
            f"Test configurations from {DATASET} dataset; "
            "fixed value for dihedral beta in alpha-gamma plane: 180 degrees",
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

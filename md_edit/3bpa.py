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
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/3bpa")
DATASET_FP = Path().cwd().parent / ("data/dataset_3BPA")
DATASET = "3BPA"

LICENSE = "https://creativecommons.org/licenses/by/4.0/"
SOFTWARE = "ORCA"
METHODS = "DFT-ωB97X"
DATA_LINK = "https://doi.org/10.1021/acs.jctc.1c00647"
PUBLICATION = "https://doi.org/10.1021/acs.jctc.1c00647"
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
ELEMENTS = ["C", "H", "O", "N"]


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

    glob_dss = [
        [
            f"{DATASET}_isolated_atoms",
            "iso_atoms.xyz",
            f"Reference C, H, O, and N atoms from {DATASET}, used to showcase "
            "the performance of linear atomic cluster expansion (ACE) force fields "
            "in a machine learning model to predict the potential energy surfaces "
            "of organic molecules.",
        ],
        [
            f"{DATASET}_test_300K",
            "test_300K.xyz",
            "Test configurations with MD simulations performed at 300K from "
            f"{DATASET}, used to showcase the performance of linear atomic "
            "cluster expansion (ACE) force fields in a machine learning model "
            "to predict the potential energy surfaces of organic molecules.",
        ],
        [
            f"{DATASET}_test_600K",
            "test_600K.xyz",
            "Test configurations with MD simulations performed at 600K from "
            f"{DATASET}, used to showcase the performance of linear atomic "
            "cluster expansion (ACE) force fields in a machine learning model "
            "to predict the potential energy surfaces of organic molecules.",
        ],
        [
            f"{DATASET}_test_1200K",
            "test_1200K.xyz",
            "Test configurations with MD simulations performed at 1200K from "
            f"{DATASET}, used to showcase the performance of linear atomic "
            "cluster expansion (ACE) force fields in a machine learning model "
            "to predict the potential energy surfaces of organic molecules.",
        ],
        [
            f"{DATASET}_train_300K",
            "train_300K.xyz",
            "Training configurations with MD simulations performed at 300K from "
            f"{DATASET}, used to showcase the performance of linear atomic "
            "cluster expansion (ACE) force fields in a machine learning model "
            "to predict the potential energy surfaces of organic molecules.",
        ],
        [
            f"{DATASET}_train_mixed",
            "train_mixedT.xyz",
            "Training configurations with MD simulation performed at 300K, 600K and "
            f"1200K from {DATASET} dataset, used to showcase the performance of linear "
            "atomic cluster expansion (ACE) force fields in a machine learning model "
            "to predict the potential energy surfaces of organic molecules.",
        ],
        [
            f"{DATASET}_test_dih_beta120",
            "test_dih_beta120.xyz",
            "Test configurations with fixed value for dihedral beta in alpha-gamma "
            f" plane of 120 degreesfrom {DATASET} dataset. Used to showcase the "
            "performance of linear atomic cluster expansion (ACE) force fields in a "
            "machine learning model to predict the potential energy surfaces of "
            "organic molecules.",
        ],
        [
            f"{DATASET}_test_dih_beta150",
            "test_dih_beta150.xyz",
            "Test configurations with fixed value for dihedral beta in alpha-gamma "
            f" plane of 150 degreesfrom {DATASET} dataset. Used to showcase the "
            "performance of linear atomic cluster expansion (ACE) force fields in a "
            "machine learning model to predict the potential energy surfaces of "
            "organic molecules.",
        ],
        [
            f"{DATASET}_test_dih_beta180",
            "test_dih_beta180.xyz",
            "Test configurations with fixed value for dihedral beta in alpha-gamma "
            f" plane of 180 degreesfrom {DATASET} dataset. Used to showcase the "
            "performance of linear atomic cluster expansion (ACE) force fields in a "
            "machine learning model to predict the potential energy surfaces of "
            "organic molecules.",
        ],
    ]

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
                "forces": {"field": "forces", "units": "eV/angstrom"},
                "_metadata": metadata,
            }
        ],
    }
    co_md_map = {"dihedrals": {"field": "dihedrals"}}

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    for glob_ds in glob_dss:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=glob_ds[1],
            generator=False,
        )
        ds_id = generate_ds_id()
        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=property_map,
                co_md_map=co_md_map,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        # cs_ids = []

        # for i, (name, regex, desc) in enumerate(cs_regexes):
        #     co_ids = client.get_data(
        #         "configurations",
        #         fields="hash",
        #         query={
        #             "hash": {"$in": all_co_ids},
        #             "names": {"$regex": regex},
        #         },
        #         ravel=True,
        #     ).tolist()

        #     print(
        #         f"Configuration set {i}",
        #         f"({name}):".rjust(22),
        #         f"{len(co_ids)}".rjust(7),
        #     )
        #     if len(co_ids) > 0:
        #         cs_id = client.insert_configuration_set(co_ids, description=desc,
        #                                                 name=name)

        #         cs_ids.append(cs_id)
        #     else:
        #         pass

        client.insert_dataset(
            # cs_ids=cs_ids,
            do_hashes=all_do_ids,
            name=glob_ds[0],
            ds_id=ds_id,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            data_license=LICENSE,
            description=glob_ds[2],
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

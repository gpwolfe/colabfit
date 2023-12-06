"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2020.144
Download link:
https://archive.materialscloud.org/record/file?filename=datasets.zip&record_id=637

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
stress
force
potential energy

Other properties added to metadata
----------------------------------
None

File notes
----------
from the readme file:
Units
  All quantities are given in these units: Å for coordinates, eV for energy, \
    eV/Å for forces and eV/Å^3 for stress.
Datasets
CA-9.db
All 48000 images generated for the CA-9 dataset.

CA-9_training.db
Training images used for training the NNP_CA-9 potential.

CA-9_validation.db
Validation images used for training the NNP_CA-9 potential.

RR_training.db
The Random-Random training images used for training the NNP_RR potential

RR_validation.db
The Random-Random validation images used for training the NNP_RR potential

BR_training.db
The Binning-Random training images used for training the NNP_BR potential

BR_validation.db
The Binning-Random validation images used for training the NNP_BR potential

BB_training.db
The Binning-Binning training images used for training the NNP_BB potential

BB_validation.db
The Binning-Binning validation images used for training the NNP_BB potential

test_images.db
Test images used to evaluate the trained NNPs


"""
from argparse import ArgumentParser
from ase.db import connect
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
    cauchy_stress_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/ca_9/ca_9_data"
)
DATASET_FP = Path().cwd().parent / ("data/ca_9")
DATASET = "CA-9"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"
PUBLICATION = "https://doi.org/10.1016/j.cartre.2021.100027"
DATA_LINK = "https://doi.org/10.24435/materialscloud:6h-yj"
LINKS = [
    "https://doi.org/10.24435/materialscloud:6h-yj",
    "https://doi.org/10.1016/j.cartre.2021.100027",
]
AUTHORS = [
    "Daniel Hedman",
    "Tom Rothe",
    "Gustav Johansson",
    "Fredrik Sandin",
    "J. Andreas Larsson",
    "Yoshiyuki Miyamoto",
]
DS_DESC = (
    "CA-9 consists of configurations of carbon with curated subsets "
    "chosen to test the effects of intentionally choosing dissimilar "
    "configurations when training neural network potentials"
)
ELEMENTS = ["C"]


def reader(filename):
    configs = []
    db = connect(filename)

    data = [image for image in db.select()]
    for i, image in enumerate(data):
        config = AtomicConfiguration(
            positions=image.positions,
            numbers=image.numbers,
            pbc=image.pbc,
            cell=image.cell,
        )
        config.info = dict(image.data)  # energy, forces, stress
        config.info["name"] = f"{filename.stem}__{i}"
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
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {"encut": {"value": 520, "units": "eV"}},
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
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV/A^3"},
                "_metadata": metadata,
            }
        ],
    }
    # name, glob string, description
    glob_dss = [
        [
            f"{DATASET}_BR_training",
            "BR_training.db",
            f"Binning-random configurations from {DATASET} dataset used for training "
            f"NNP_BR potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_BR_validation",
            "BR_validation.db",
            f"Binning-random configurations from {DATASET} dataset used during "
            f"validation step for NNP_BR potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_BB_training",
            "BB_training.db",
            f"Binning-binning configurations from {DATASET} dataset used for training "
            f"NNP_BB potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_BB_validation",
            "BB_validation.db",
            f"Binning-binning configurations from {DATASET} dataset used during "
            f"validation step for NNP_BB potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_RR_validation",
            "RR_validation.db",
            f"Random-random configurations from {DATASET} dataset used during "
            f"validation step for NNP_RR potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_RR_training",
            "RR_training.db",
            f"Random-random configurations from {DATASET} dataset used for training "
            f"NNP_RR potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_training",
            "CA-9_training.db",
            f"Configurations from {DATASET} dataset used for training NNP_CA-9 "
            f"potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_validation",
            "CA-9_validation.db",
            f"Configurations from {DATASET} dataset used during validation step for "
            f"NNP_CA-9 potential. {DS_DESC}",
        ],
        [
            f"{DATASET}_test",
            "test_images.db",
            f"Test configurations from {DATASET} dataset used to evaluate trained NNPs."
            f"{DS_DESC}",
        ],
    ]
    for ds_name, glob_str, description in glob_dss:
        print(ds_name)
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=glob_str,
            generator=False,
        )
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
            name=ds_name,
            ds_id=ds_id,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=description,
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

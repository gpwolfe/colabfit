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
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
    cauchy_stress_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("ca_9_data")
DATASET = "CA-9"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"
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
DS_DESC = "Approximately 50,000 configurations of carbon with curated subsets\
 chosen to test the effects of intentionally choosing dissimilar\
 configurations when training neural network potentials"
ELEMENTS = ["C"]
GLOB_STR = "*.db"


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
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
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
                "stress": {"field": "stress", "units": "eV/A"},
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
            f"{DATASET}-BB-training",
            "BB_training.*",
            f"Binning-binning configurations from {DATASET} dataset used for training "
            "NNP_BB potential",
        ],
        [
            f"{DATASET}-BB-validation",
            "BB_validation.*",
            f"Binning-binning configurations from {DATASET} dataset used during "
            "validation step for NNP_BB potential",
        ],
        [
            f"{DATASET}-BR-training",
            "BR_training.*",
            f"Binning-random configurations from {DATASET} dataset used for training "
            "NNP_BR potential",
        ],
        [
            f"{DATASET}-BR-validation",
            "BR_validation.*",
            f"Binning-random configurations from {DATASET} dataset used during "
            "validation step for NNP_BR potential",
        ],
        [
            f"{DATASET}-CA_9-training",
            "CA-9_training.*",
            f"Configurations from {DATASET} dataset used for training NNP_CA-9 "
            "potential",
        ],
        [
            f"{DATASET}-CA_9-validation",
            "CA-9_validation.*",
            f"Configurations from {DATASET} dataset used during validation step for "
            "NNP_CA-9 potential",
        ],
        [
            f"{DATASET}-all",
            "CA-9__.*",
            f"Complete configuration set from {DATASET} dataset",
        ],
        [
            f"{DATASET}-RR-training",
            "RR_training.*",
            f"Random-random configurations from {DATASET} dataset used for training "
            "NNP_RR potential",
        ],
        [
            f"{DATASET}-RR-validation",
            "RR_validation.*",
            f"Random-random configurations from {DATASET} dataset used during "
            "validation step for NNP_RR potential",
        ],
        [
            f"{DATASET}-test",
            "test_images.*",
            f"Test configurations from {DATASET} dataset used to evaluate trained NNPs",
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

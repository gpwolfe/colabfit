"""
author: Gregory Wolfe

Properties
----------
energy
forces


Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/water_ice_nep_2023")
DATASET_NAME = "water_ice_NEP_2023"

SOFTWARE = "CP2K"
METHODS = "DFT-revPBE0-D3"
PUBLICATION = "https://doi.org/10.26434/chemrxiv-2023-sr496"
DATA_LINK = "https://github.com/ZKC19940412/water_ice_nep"
LINKS = [
    "https://github.com/ZKC19940412/water_ice_nep",
    "https://doi.org/10.26434/chemrxiv-2023-sr496",
]
AUTHORS = [
    "Zekun Chen",
    "Margaret L. Berrens",
    "Kam-Tung Chan",
    "Zheyong Fan",
    "Davide Donadio",
]
DATASET_DESC = (
    "The main part of the dataset consists of structures of liquid water "
    "at 300 K from first-principles molecular dynamics (FPMD) simulations "
    "using a hybrid density functional with dispersion corrections. The dataset "
    "is expanded to include nuclear quantum effects by adding structures from "
    "path-integral molecular dynamics (PIMD) simulations. The final dataset "
    "contains 814 structures of liquid water at different temperatures and "
    "pressures, water slab, and ice Ih and ice VIII. These systems cover a "
    "wide range of structural and dynamical properties of water and ice. "
    "This dataset builds on the dataset from Schran, et al (2020) "
    "https://doi.org/10.1063/5.0016004"
)
ELEMENTS = None
GLOB_STR = "*.xyz"

# Assign additional relevant property instance metadata, such as basis set used
PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    # "basis-set": {"field": "basis_set"}
}

# Define dynamic 'field' -> value relationships or static 'value' -> value relationships
# for your properties here. Any "field" value should be contained in your PI_METADATA
# In this example, the custom reader function should return ase.Atoms objects with
# atoms.info['energy'] and atoms.info['forces'] or atoms.arrays['forces'] values.

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "kbar"},
    #         "volume-normalized": {"value": True, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

# CSS = [
#     [
#         f"{DATASET_NAME}_aluminum",
#         {"names": {"$regex": "aluminum"}},
#         f"Configurations of aluminum from {DATASET_NAME} dataset",
#     ]
# ]


def reader(filepath: Path):
    configs = read(filepath, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = "water_ice_nep_2023_{i}"
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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            # co_md_map=CO_METADATA,
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
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

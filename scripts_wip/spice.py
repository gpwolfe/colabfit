"""
author:

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
These keys appear in all configs:

Read once:
[('atomic_numbers', (27,)),
('smiles', (1,)),
('subset', (1,)),

Iterate over zip:
('conformations', (50, 27, 3)),
('dft_total_energy', (50,)),
('dft_total_gradient', (50, 27, 3)),
('formation_energy', (50,)),
('mayer_indices', (50, 27, 27)),
('scf_dipole', (50, 3)),
('scf_quadrupole', (50, 3, 3)),
('wiberg_lowdin_indices', (50, 27, 27))
('wiberg_lowdin_indices', (50, 27, 27))]

These appear in most but not all:
Iterate over zip:
('mbis_charges', (50, 27, 1)),
('mbis_dipoles', (50, 27, 3)),
('mbis_octupoles', (50, 27, 3, 3, 3)),
('mbis_quadrupoles', (50, 27, 3, 3)),
"""
from argparse import ArgumentParser
import h5py
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


DATASET_FP = Path("").cwd()
DATASET_NAME = ""

SOFTWARE = ""
METHODS = ""
LINKS = ["", ""]
AUTHORS = [""]
DATASET_DESC = ""
ELEMENTS = [""]
GLOB_STR = "*.*"

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

# Define any configuration-specific metadata here.
CO_METADATA = {
    "enthalpy": {"field": "h", "units": "Ha"},
    "zpve": {"field": "zpve", "units": "Ha"},
}


def reader(filepath: Path):
    configs = read(filepath, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{filepath.stem}_{i}"
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
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # If no obvious divisions between configurations exist (i.e., different methods or
    # materials), remove the following lines through 'cs_ids.append(...)' and from
    # 'insert_dataset(...) function remove 'cs_ids=cs_ids' argument.

    cs_regexes = [
        [
            f"{DATASET_NAME}_aluminum",
            "aluminum",
            f"Configurations of aluminum from {DATASET_NAME} dataset",
        ]
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
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


CO_MD = [
    # 'atomic_numbers',
    #  'conformations',
    #  'dft_total_energy',
    #  'formation_energy',
    "dft_total_gradient",
    "mayer_indices",
    "mbis_charges",  # Not all
    "mbis_dipoles",  # Not all
    "mbis_octupoles",  # Not all
    "mbis_quadrupoles",  # Not all
    "scf_dipole",
    "scf_quadrupole",
    "smiles",
    "subset",
    "wiberg_lowdin_indices",
]

if __name__ == "__main__":
    main(sys.argv[1:])

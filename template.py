"""
author:

Properties
----------

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


DATASET_FP = Path("").cwd()
DATASET_NAME = ""

SOFTWARE = ""
METHODS = ""

PUBLICATION = ""
DATA_LINK = ""
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
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "GPa"},
    #         "volume-normalized": {"value": True, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

# Define any configuration-specific metadata here.
CO_METADATA = {
    "enthalpy": {"field": "h", "units": "hartree"},
    "zpve": {"field": "zpve", "units": "hartree"},
}

# If obvious configuration divisions exist, create mapping
# In this example, the configurations set name is the first element;
# the second element is the query with which to select configurations -- often
# this queries over the "name" field, as shown.
# The third element is the configuration set description. This should be human-readable
# If no divisions exist, simply set CSS = None
CSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]


def reader(filepath: Path):
    """
    If using a customer reader function, define here.

    Reader function should accept only one argument--a Path() object--and return
    either a list or generator of AtomicConfiguration objects or ase.Atoms objects.
    Examples of custom reader functions can be found in the finished scripts
    directories.

    Below is a minimal example using ase.io.read to parse e.g., an extxyz file.
    If the extxyz header contains the fields defined in PROPERTY_MAP and CO_METADATA
    above (i.e., 'energy' and 'forces'; and 'h' and 'zpve', respectively), these fields
    will be used in the data ingestion process to create property-instance -> PI-medata
    relationships and configuration -> CO-metadata relationships.
    """
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
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # If no obvious divisions between configurations exist (i.e., different methods or
    # materials), remove the following lines through 'cs_ids.append(...)' and from
    # 'insert_dataset(...) function remove 'cs_ids=cs_ids' argument.

    cs_ids = []
    if CSS:
        for i, (name, query, desc) in enumerate(CSS):
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=name,
                description=desc,
                query=query,
            )

            cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

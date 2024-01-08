"""
author: Gregory Wolfe

Properties
----------
forces
free-energy

Other properties added to metadata
----------------------------------

File notes
----------
outcar files provided, custom parser written for vasp settings


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


DATASET_FP = Path("data/geng_goddard")
DATASET_NAME = "N_and_O_columns_non-bonded_potential_JCP2023"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1063/5.0174188"
DATA_LINK = "https://doi.org/10.1063/5.0174188"
# OTHER_LINKS = []

AUTHORS = ["Peng Geng", "Sergey Zybin", "Saber Naserifar", "William A. Goddard, III"]
DATASET_DESC = (
    "This dataset contains structures of materials from the N (15th), "
    "O (16th) and F (16th) columns of the periodic table for use in generating "
    "a 2-body non-bonded vdW potential."
)
ELEMENTS = None
GLOB_STR = "OUTCAR*"

PI_METADATA = {
    "software": {"value": ""},
    "method": {"value": ""},
    "input": {"field": "input"}
    # "input": {"value": {}}
}

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

CO_METADATA = {
    "enthalpy": {"field": "h", "units": "Ha"},
    "zpve": {"field": "zpve", "units": "Ha"},
}

CSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
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
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_ids = []
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
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

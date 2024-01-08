"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
Lattice
Properties=species:S:1:pos:R:3:
forces:R:3
energy
stress
free_energy
pbc

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("").cwd()
DATASET_NAME = "SAIT_semiconductors_2023"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = ""
DATA_LINK = "https://github.com/SAITPublic/MLFF-Framework"
LINKS = ["https://github.com/SAITPublic/MLFF-Framework", ""]
AUTHORS = [
    "Geonu Kim",
    "Byunggook Na",
    "Gunhee Kim",
    "Hyuntae Cho",
    "Seung-Jin Kang",
    "Hee Sun Lee",
    "Saerom Choi",
    "Heejae Kim",
    "Seungwon Lee",
    "Yongdeok Kim",
]
DATASET_DESC = (
    "Two rich datasets for important semiconductor thin film materials silicon "
    "nitride (SiN) and hafnium oxide (HfO) are introduced to foster the development "
    "of MLFF for the semiconductors. We conducted DFT simulations with various "
    "conditions that include initial structures, stoichiometry, temperature, strain, "
    "and defects."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    # "basis-set": {"field": "basis_set"}
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
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "kbar"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "enthalpy": {"field": "h", "units": "hartree"},
    "zpve": {"field": "zpve", "units": "hartree"},
}

CSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]

DSS = (
    (
        "SAIT_semiconductors_2023_train",
        {"name": {"$regex": "_train"}},
        "*_raw.xyz",
        "Training configurations from SAIT_semiconductors_2023 dataset. "
        + DATASET_DESC,
    ),
    (
        "SAIT_semiconductors_2023_test",
        {"name": {"$regex": "_test"}},
        "Testset",
        "Test configurations from SAIT_semiconductors_2023 dataset. " + DATASET_DESC,
    ),
)


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
    client.insert_property_definition(cauchy_stress_pd)

    for ds_name, ds_query, ds_glob, ds_desc in DSS:
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
            links=LINKS,
            description=DATASET_DESC,
            verbose=True,
            cs_ids=cs_ids,  # remove line if no configuration sets to insert
        )


if __name__ == "__main__":
    main(sys.argv[1:])

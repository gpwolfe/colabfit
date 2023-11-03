"""
author: Gregory Wolfe

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


DATASET_FP = Path("data/zrsn_jnm_2024")
DATASET_NAME = "Zr_Sn_JNM_2024"

SOFTWARE = ""
METHODS = ""

PUBLICATION = "https://doi.org/10.1016/j.jnucmat.2023.154794"
DATA_LINK = "https://github.com/meihaojie/Zr_Sn_system"
LINKS = [
    "https://github.com/meihaojie/Zr_Sn_system",
    "https://doi.org/10.1016/j.jnucmat.2023.154794",
]
AUTHORS = [
    "Haojie Mei",
    "Liang Chen",
    "Feifei Wang",
    "Guisen Liu",
    "Jing Hu",
    "Weitong Lin",
    "Yao Shen",
    "Jinfu Li",
    "Lingti Kong",
]
DATASET_DESC = (
    "This dataset contains data from density functional theory calculations "
    "of various atomic configurations of pure Zr, pure Sn, and Zr-Sn alloys "
    "with different structures, defects, and compositions. Energies, "
    "forces, and stresses are calculated at the DFT level of theory. The dataset "
    "is divided into training and validation sets with an 80/20 split. "
    "Includes 23,956 total configurations."
)
ELEMENTS = [""]
GLOB_STR = "*.*"

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

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
    # atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/mc_oxygen-vacancy_defects_in_cu2o/mc_data/dataset_no_duplicates"
)
DATASET_NAME = "oxygen-vacancy_defects_in_Cu2O(111)"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "http://doi.org/10.1088/2516-1075/ace0aa"
DATA_LINK = "https://doi.org/10.24435/materialscloud:3z-bk"
# OTHER_LINKS = []

AUTHORS = ["Nanchen Dongfang", "Marcella Iannuzzi", "Yasmine Al-Hamdani"]
DATASET_DESC = (
    "This dataset investigates the effect of defects, such as copper and oxygen "
    "vacancies, in cuprous oxide films. Structures include oxygen vacancies formed "
    "in proximity of a reconstructed Cu2O(111) surface, where the outermost "
    "unsaturated copper atoms are removed, thus forming non-stoichiometric surface "
    "layers with copper vacancies. Surface and bulk properties are addressed by "
    "modelling a thick and symmetric slab consisting of 8 atomic layers and 736 atoms. "
    "Configuration sets include bulk, slab, vacancy and oxygen gas. Version v1"
)
ELEMENTS = None
GLOB_STR = "*.xyz"

PI_METADATA = {
    "software": {"value": "CP2K"},
    "method": {"value": "DFT-PBE+U+D3"},
    "input": {
        "value": {
            "primary-basis-set": "DZVP-MOLOPT-GTH",
            "auxiliary-basis-set": {"O": "cFIT3", "Cu": "cFIT9"},
            "plane-wave-cutoff": {"value": 600, "units": "Ry"},
            "energy-convergence": {"value": 5e-7, "units": "hartree"},
            "force-convergence": {"value": 1e-3, "units": "hartree/bohr"},
        }
    },
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "E", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    # "atomic-forces": [
    #     {
    #         "forces": {"field": "forces", "units": "eV/angstrom"},
    #         "_metadata": PI_METADATA,
    #     },
    # ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "eV/angstrom^3"},
    #         "volume-normalized": {"value": False, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

# CO_METADATA = {
#     "enthalpy": {"field": "h", "units": "Ha"},
#     "zpve": {"field": "zpve", "units": "Ha"},
# }

CSS = [
    [
        f"{DATASET_NAME}_bulk",
        {"names": {"$regex": "bulk"}},
        f"Bulk configurations of Cu2O from {DATASET_NAME}",
    ],
    [
        f"{DATASET_NAME}_slab",
        {"names": {"$regex": "slab"}},
        f"Slab configurations of Cu2O from {DATASET_NAME}",
    ],
    [
        f"{DATASET_NAME}_oxygen_vacancy",
        {"names": {"$regex": "ovac"}},
        f"Configurations of Cu2O with oxygen vacancy from {DATASET_NAME}",
    ],
    [
        f"{DATASET_NAME}_O2",
        {"names": {"$regex": "o2_gas"}},
        f"O2 gas configurations from {DATASET_NAME}",
    ],
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

    # client.insert_property_definition(atomic_forces_pd)
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

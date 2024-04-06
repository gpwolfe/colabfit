"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
./stoichiometric contains the data for  stoichiometric LaMnO3 (LMO) unstrained \
    and epitaxially or isostatically strained structures
./V_OOP contains results for the neutral OP oxygen vacancy in the unstrained and \
    epitaxially or isostatically strained LMO structures
./V_OIP contains results for the neutral IP oxygen vacancy in the unstrained and \
    epitaxially or isostatically strained LMO structures
./singly_charged_VO contains results for the singly charged oxygen vacancy
./doubly_charged_VO contains results for the doubly charged oxygen vacancy
./O2 contains the data relative to the O2 molecule used to compute the defect \
    formation energies
./cplap contains the results of the phase diagram determination
./MnO contains the data for the MnO calculation
./Mn2O3 contains the data for the Mn2O3 calculation
./Mn_metal contains the data for the Mn metal calculation
./La_metal contains the data for the La metal calculation
./La2O3 contains the data for the La2O3 calculation
./Mn3O4 contains the data for the Mn3O4 calculation
./BaTiO3 contained the data for the Berry phase calculation in BaTiO3

"""

from argparse import ArgumentParser
import functools
import logging
import time
from pathlib import Path
import sys
from ase.calculators.calculator import PropertyNotImplementedError
from ase.io import iread
import pymongo

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/local_polarization_in_oxygen-deficient_lamno3_induced_by_charge_"
    "localization_in_the_jahn-teller_distorted_structure"
)
DATASET_NAME = "local_polarization_in_oxygen-deficient_LaMnO3_PRR2020"
LICENSE = "CC-BY-4.0"
PUB_YEAR = "2020"

PUBLICATION = "http://doi.org/10.1103/PhysRevResearch.2.042040"
DATA_LINK = "https://doi.org/10.24435/materialscloud:m9-9d"
# OTHER_LINKS = []

AUTHORS = ["Chiara Ricca", "Nicolas Niederhauser", "Ulrich Aschauer"]
DATASET_DESC = (
    "This dataset contains structural calculations of LaMnO3 carried out in "
    "Quantum ESPRESSO at the DFT-PBEsol+U level of theory. The dataset was built to "
    "explore strained and stoichiometric and oxygen-deficient LaMnO3."
)
ELEMENTS = None
GLOB_STR = "pw.out"

PI_METADATA = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE+U"},
    "input": {"field": "input"},
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
            "stress": {"field": "stress", "units": "kilobar"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


CSS = [
    [
        f"{DATASET_NAME}_stoichiometric",
        {"names": {"$regex": "stoichiometric"}},
        f"Configurations from {DATASET_NAME} of stoichiometric LaMnO3 (LMO): "
        "unstrained and epitaxially or isostatically strained structures",
    ],
    [
        f"{DATASET_NAME}_V_OOP",
        {"names": {"$regex": "V_OOP"}},
        f"Configurations from {DATASET_NAME} of the neutral OP oxygen vacancy in "
        "the unstrained and epitaxially or isostatically strained LMO structures",
    ],
    [
        f"{DATASET_NAME}_V_OIP",
        {"names": {"$regex": "V_OIP"}},
        f"Configurations from {DATASET_NAME} of the neutral IP oxygen vacancy in "
        "the unstrained and epitaxially or isostatically strained LMO structures",
    ],
    [
        f"{DATASET_NAME}_singly_charged_VO",
        {"names": {"$regex": "singly_charged_VO"}},
        f"Configurations from {DATASET_NAME} with singly charged oxygen vacancy",
    ],
    [
        f"{DATASET_NAME}_doubly_charged_VO",
        {"names": {"$regex": "doubly_charged_VO"}},
        f"Configurations from {DATASET_NAME} with doubly charged oxygen vacancy",
    ],
    [
        f"{DATASET_NAME}_O2",
        {"names": {"$regex": "O2"}},
        f"Configurations from {DATASET_NAME} of the O2 molecule used to compute the "
        "defect formation energies",
    ],
    [
        f"{DATASET_NAME}_MnO",
        {"names": {"$regex": "MnO"}},
        f"Configurations from {DATASET_NAME} of the MnO calculation",
    ],
    [
        f"{DATASET_NAME}_Mn2O3",
        {"names": {"$regex": "Mn2O3"}},
        f"Configurations from {DATASET_NAME} of the Mn2O3 calculation",
    ],
    [
        f"{DATASET_NAME}_Mn_metal",
        {"names": {"$regex": "Mn_metal"}},
        f"Configurations from {DATASET_NAME} of the Mn metal calculation",
    ],
    [
        f"{DATASET_NAME}_La_metal",
        {"names": {"$regex": "La_metal"}},
        f"Configations from {DATASET_NAME} of the La metal calculation",
    ],
    [
        f"{DATASET_NAME}_La2O3",
        {"names": {"$regex": "La2O3"}},
        f"Configurations from {DATASET_NAME} of the La2O3 calculation",
    ],
    [
        f"{DATASET_NAME}_Mn3O4",
        {"names": {"$regex": "Mn3O4"}},
        f"Configurations from {DATASET_NAME} of the Mn3O4 calculation",
    ],
    [
        f"{DATASET_NAME}_BaTiO3",
        {"names": {"$regex": "BaTiO3"}},
        f"Configurations from {DATASET_NAME} of the Berry phase calculation in BaTiO3",
    ],
]


def namer(fp):
    ds_fp_str = "__".join(DATASET_FP.absolute().parts).replace("/", "")
    name = "__".join(fp.absolute().parts[:-1]).replace("/", "")
    name = name.replace(ds_fp_str + "__", "")
    return name


def reader(fp):
    for config in iread(fp, index=":"):
        config.info["forces"] = config.get_forces()
        try:
            config.info["stress"] = config.get_stress(voigt=False)
        except PropertyNotImplementedError:
            continue
        config.info["energy"] = config.get_potential_energy()
        config.info["name"] = namer(fp)
        yield config


MAX_AUTO_RECONNECT_ATTEMPTS = 100


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                if wait_t > 1800:
                    wait_t = 1800  # cap at 1/2 hour
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


@auto_reconnect
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
        args.db_name, nprocs=32, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()
    print(f"Dataset ID: {ds_id}\nDS Name: {DATASET_NAME}")

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
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        other_links=None,
        description=DATASET_DESC,
        publication_year=PUB_YEAR,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

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
import functools
import logging
import time
from pathlib import Path
import sys

from ase.io import Trajectory
import pymongo

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/on-the-fly_assessment_of_diffusion_barriers_of_disordered_transition_metal_"
    "oxyfluorides_using_local_descriptors"
)
DATASET_NAME = "disordered_transition_metal_oxyfluorides_EA2021"
LICENSE = "CC-BY-4.0"
PUB_YEAR = "2021"

PUBLICATION = "http://doi.org/10.1016/j.electacta.2021.138551"
DATA_LINK = "https://doi.org/10.24435/materialscloud:9v-3q"
# OTHER_LINKS = []

AUTHORS = [
    "Jin Hyun Chang",
    "Peter Bjørn Jørgensen",
    "Simon Loftager",
    "Arghya Bhowmik",
    "Juan María García Lastra",
    "Tejs Vegge",
]
DATASET_DESC = (
    'Data from "On-the-fly assessment of diffusion barriers of disordered transition '
    'metal oxyfluorides using local descriptors". The dataset contains the result of '
    "48 Nudged Elastic Band calculations of Li(2-x)VO2F diffusion barriers. The NEB "
    "was performed with VASP, using projector augmented-wave (PAW) method to "
    "describe electron-ion interaction. The disordered rock salt cells were created "
    "using a 3 x 4 x 4 supercell containing 96 atoms (in case of no vacancies). PBE "
    "is used as XC functional while a rotationally invariant Hubbard U correction "
    "was applied to the d orbital of V with a U value of 3.25 eV."
)
ELEMENTS = None
GLOB_STR = "*.traj"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE+U"},
    "input": {
        "value": {
            "ENCUT": "500 eV",
            "EDIFFG": "2E-02 eV/angstrom",
            "kpoints-scheme": "Monkhorst-Pack",
            "kpoints-density": "3x3x3",
        }
    },
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
    #         "stress": {"field": "stress", "units": "kilobar"},
    #         "volume-normalized": {"value": False, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

# CO_METADATA = {
#     "enthalpy": {"field": "h", "units": "Ha"},
#     "zpve": {"field": "zpve", "units": "Ha"},
# }

# CSS = [
#     [
#         f"{DATASET_NAME}_aluminum",
#         {"names": {"$regex": "aluminum"}},
#         f"Configurations of aluminum from {DATASET_NAME} dataset",
#     ]
# ]


def reader(filepath: Path):
    configs = []
    for traj in Trajectory(filepath, "r"):
        traj.info["energy"] = traj.get_potential_energy()
        traj.info["forces"] = traj.get_forces()
        traj.info["name"] = f"{DATASET_NAME}_{filepath.stem}"
        configs.append(traj)
    print(len(configs))
    return configs


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
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

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

    # cs_ids = []
    # for i, (name, query, desc) in enumerate(CSS):
    #     cs_id = client.query_and_insert_configuration_set(
    #         co_hashes=all_co_ids,
    #         ds_id=ds_id,
    #         name=name,
    #         description=desc,
    #         query=query,
    #     )

    #     cs_ids.append(cs_id)

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
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

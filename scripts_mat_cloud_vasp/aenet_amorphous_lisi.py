"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""

import functools
import logging
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

import pymongo
from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, generate_ds_id, load_data
from colabfit.tools.property_definitions import (  # cauchy_stress_pd,
    atomic_forces_pd,
    potential_energy_pd,
)

DATASET_FP = Path(
    "data/aenet-lammps_and_aenet-tinker:_interfaces_for_accurate_and_efficient_"
    "molecular_dynamics_simulations_with_machine_learning_potentials/amorphous-LiSi-PBE"
)
DATASET_NAME = "AENET_amorphous_LiSi_JCP2021"
LICENSE = "CC-BY-4.0"
PUB_YEAR = "2020"

PUBLICATION = "http://doi.org/10.1063/5.0063880"
DATA_LINK = "https://doi.org/10.24435/materialscloud:dx-ct"
# OTHER_LINKS = []

AUTHORS = [
    "Michael S. Chen",
    "Tobias Morawietz",
    "Thomas E. Markland",
    "Nongnuch Artrith",
]
DATASET_DESC = (
    "The amorphous LiSi data set comprises 45,169 atomic structures with "
    "compositions Li(x)Si (0.0≤x≤4.75) and the corresponding energies and "
    "interatomic forces, which were generated using an iterative approach "
    "based on an evolutionary algorithm and subsequent refinement, as "
    "described in detail in reference [15]. The data includes bulk, surface, "
    "and cluster structures with system sizes of up to 608 atoms. "
    "The energies and forces of the LiSi structures were obtained from DFT "
    "calculations using the Perdew-Burke-Ernzerhof [10] exchange-correlation "
    "functional and projector-augmented wave pseudopotentials [16], as "
    "implemented in the Vienna Ab-Initio Simulation Package (VASP) "
    "[17,18]. We employed a plane-wave basis set with an energy cutoff of 520 "
    "eV for the representation of the wavefunctions and a uniform "
    "gamma-centered k-point grid for the Brillouin zone integration, with a "
    "mesh density corresponding to a number of k points of at least 1000 "
    "divided by the number of atoms. The atomic positions and lattice "
    "parameters of all structures were optimized until residual forces were "
    "below 20 meV/Å. This dataset was also used for the construction of the "
    "ANN potential in Ref. [15] and [19]. "
    "[10] J. P. Perdew, K. Burke, and M. Ernzerhof, Phys. Rev. Lett. 77, 3865 (1996). "
    "[15] N. Artrith, A. Urban, G. Ceder, J. Chem. Phys. 148 (2018) 241711. "
    "[16] P. E. Blöchl, Phys. Rev. B 50, 17953–17979 (1994). "
    "[17] G. Kresse, J. Furthmüller, Phys. Rev. B 54, 11169–11186 (1996). "
    "[18] Kresse, J. Furthmüller, Comput. Mater. Sci. 6, 15–50 (1996). "
    "[19] N. Artrith, A. Urban, Y. Wang, G. Ceder, arXiv:1901.09272, "
    "https://arxiv.org/pdf/1901.09272.pdf"
)
ELEMENTS = None
GLOB_STR = "*.xsf"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "ENCUT": "520 eV",
            "EDIFFG": "2E-3 eV/Ang",
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


def reader(filepath: Path):
    name = f"{DATASET_NAME}_{filepath.stem}"
    with filepath.open("r") as f:
        header = f.readline()
        energy = float(header.strip().split()[-2])
    config = read(filename=filepath, format="xsf")
    config.info["energy"] = energy
    config.info["name"] = name
    config.info["forces"] = config.get_forces()
    return [config]


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
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=True,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
        publication_year=PUB_YEAR,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

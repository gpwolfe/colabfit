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
    "data/structure_database_of_glass-ceramic_lithium_thiophosphate_electrolytes"
)
DATASET_NAME = "glass-ceramic_lithium_thiophosphate_electrolytes_"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1021/acs.chemmater.2c00267"
DATA_LINK = "https://doi.org/10.24435/materialscloud:j5-tz"
# OTHER_LINKS = []

AUTHORS = ["Haoyue Guo", "Nongnuch Artrith"]
DATASET_DESC = (
    "This database contains computationally generated atomic structures of "
    "glass-ceramics lithium thiophosphates (gc-LPS) with the general composition "
    "(Li2S)x(P2S5)1-x. Total energies "
    "and interatomic forces from density-functional theory (DFT) calculations are "
    "included. "
    "The DFT calculations used projector-augmented-wave (PAW) pseudopotentials "
    "and the Perdew-Burke-Ernzerhof (PBE) exchange-correlation functional as "
    "implemented in the Vienna Ab Initio Simulation Package (VASP) and a "
    "kinetic energy cutoff of 520 eV. The first Brillouin zone was sampled using "
    "VASP's fully automatic k-point scheme with a length parameter Rk = 25Å. "
    "The gc-LPS structures were generated using a combination of different sampling "
    "methods. Initial amorphous structure models were generated with ab initio "
    "molecular dynamics (AIMD) simulations of supercells at 1200 K using a Nose-"
    "Hoover thermostat with a time step of 1 fs. To obtain near-ground-state "
    "structures as reference for the machine-learning potential, 150 evenly spaced "
    "snapshots were extracted from the AIMD trajectories that were reoptimized with "
    "DFT geometry optimizations at zero Kelvin. Additional structures were generated "
    "by scaling the lattice parameters of the crystalline LPS structures (see below) "
    "by ±15% and perturbing atomic positions in AIMD simulations as described above."
    "The resulting database was used to train a specialized ANN potential for the "
    "sampling of structures along the Li2S-P2S5 composition line with a "
    "genetic-algorithm (GA) as implemented in the atomistic evolution (ævo) package, "
    "following a previously reported protocol. Starting from supercells of "
    "the ideal crystal structures, either Li and S atoms were removed with a ratio "
    "of 2:1, or P and S atoms were removed with a ratio of 2:5, and low-energy "
    "configurations were determined with GA sampling. A population size of 32 trials "
    "and a mutation rate of 10% were employed. The ANN potential was iteratively "
    "refined by including additional sampled structures in the training. For each "
    "composition, at least 10 lowest energy structure models identified with the "
    "ANN-GA approach were selected and fully relaxed with DFT."
    "Also included in the present database are the XSF files of the previously "
    "reported crystalline phases LiPS3, Li2PS3, Li4P2S7, Li7P3S11, α-Li3PS4, "
    "β-Li3PS4, γ-Li3PS4, and Li48P16S61. The "
    "crystal structures were obtained from the Inorganic Crystal Structure Database "
    "(ICSD). the Materials Project (MP) database, the Open Quantum Materials Database "
    "(OQMD), and the AFLOW database. The configuration names indicate the "
    "journal reference and the database."
)
ELEMENTS = None
GLOB_STR = "*.xsf"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "ENCUT": "520 eV",
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


CSS = [
    [
        f"{DATASET_NAME}_structures_of_crystalline_LPS_phases",
        {"names": {"$regex": "structures-of-crystalline-lps-phases"}},
        f"Structures of (Li2S)x(P2S5)1-x of crystalline LPS phases from {DATASET_NAME} "
        "dataset",
    ],
    [
        f"{DATASET_NAME}_structures_of_glassy-ceramic_LPS_phases",
        {"names": {"$regex": "structures-of-glassy-ceramic-lps-phases"}},
        f"Structures of glassy-ceramic LPS phases from {DATASET_NAME} dataset",
    ],
]


def namer(fp):
    ds_fp_str = "__".join(DATASET_FP.absolute().parts).replace("/", "")
    name = (
        "__".join(fp.absolute().parts[:-1])
        .replace(ds_fp_str + "__", "")
        .replace("/", "")
    )
    return name


def reader(filepath: Path):
    name = namer(filepath)
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

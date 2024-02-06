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

from ase.io.vasp import read_vasp
import pymongo

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/hydrogen-induced_insulating_state_accompanied_by_"
    "inter-layer_charge_ordering_in_smnio3/"
)
DATASET_NAME = "Hydrogen-induced_insulating_state_SmNiO3"
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "https://doi.org/10.48550/arXiv.2210.07656"
DATA_LINK = "https://doi.org/10.24435/materialscloud:4w-qm"
# OTHER_LINKS = []

AUTHORS = ["Kunihiko Yamauchi", "Ikutaro Hamada"]
DATASET_DESC = (
    "A dataset of DFT-calculated energies created to investigate the effect of "
    "hydrogen doping on the crystal structure and the electronic state in SmNiO3."
    "Configuration sets include sets for apically and side-bonded hydrogen atoms for "
    "1-9 hydrogen atoms."
)
ELEMENTS = None
GLOB_STR = "energy.dat"

PI_METADATA = {
    "software": {"value": "VASP"},
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
        f"{DATASET_NAME}_1H_apical",
        {"names": {"$regex": "1Hap"}},
        "Configurations with 1 H atom bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_1H_side",
        {"names": {"$regex": "1Hsd"}},
        "Configurations with 1 H atom bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_2H_apical",
        {"names": {"$regex": "2Hap"}},
        "Configurations with 2 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_2H_side",
        {"names": {"$regex": "2Hsd"}},
        "Configurations with 2 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_3H_apical",
        {"names": {"$regex": "3Hap"}},
        "Configurations with 3 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_3H_side",
        {"names": {"$regex": "3Hsd"}},
        "Configurations with 3 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_4H_apical",
        {"names": {"$regex": "4Hap"}},
        "Configurations with 4 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_4H_side",
        {"names": {"$regex": "4Hsd"}},
        "Configurations with 4 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_5H_apical",
        {"names": {"$regex": "5Hap"}},
        "Configurations with 5 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_5H_side",
        {"names": {"$regex": "5Hsd"}},
        "Configurations with 5 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_6H_apical",
        {"names": {"$regex": "6Hap"}},
        "Configurations with 6 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_6H_side",
        {"names": {"$regex": "6Hsd"}},
        "Configurations with 6 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_7H_apical",
        {"names": {"$regex": "7Hap"}},
        "Configurations with 7 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_7H_side",
        {"names": {"$regex": "7Hsd"}},
        "Configurations with 7 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_8H_apical",
        {"names": {"$regex": "8Hap"}},
        "Configurations with 8 H atoms bonded to apical oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_8H_side",
        {"names": {"$regex": "8Hsd"}},
        "Configurations with 8 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_9H_side",
        {"names": {"$regex": "9Hsd"}},
        "Configurations with 9 H atoms bonded to side oxygen from "
        f"{DATASET_NAME} dataset",
    ],
]


# EN_FILE = DATASET_FP / "vasp_output/energy.dat"
KPOINTS_FILE = DATASET_FP / "vasp_input/KPOINTS"
INCAR_FILE = DATASET_FP / "vasp_input/INCAR_scf"
VASP_DIR = DATASET_FP / "vasp_structure"


def parse_incar(fp):
    with open(fp, "r") as f:
        lines = f.readlines()
    incar = dict()
    for line in lines:
        if "=" in line:
            keyvals = line.split("=")
            key = keyvals[0].strip()
            value = "".join(keyvals[1:]).strip().split("#")[0].strip()
            incar[key] = value
    return incar


def get_kpoints(fp):
    with open(fp, "r") as f:
        # f.readline() # if skipping first line
        kpoints = "".join(f.readlines())
    return kpoints


def reader(filepath: Path):
    file_energies = dict()
    with open(filepath, "r") as f:
        for line in f:
            file_energies[f"{'_'.join(line.split()[0:2])}.vasp"] = float(
                line.split()[2]
            )
    incar = parse_incar(INCAR_FILE)
    kpoints = get_kpoints(KPOINTS_FILE)
    vasp_files = list(VASP_DIR.glob("*.vasp"))
    for i, vasp_file in enumerate(vasp_files):
        if vasp_file.name == "SmNiO3.vasp":
            pass
        else:
            atoms = read_vasp(vasp_file)
            atoms.info["input"] = {"incar": incar, "kpoints": kpoints}
            atoms.info["energy"] = file_energies[vasp_file.name]
            atoms.info["name"] = f"{vasp_file.stem}"
            yield atoms


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

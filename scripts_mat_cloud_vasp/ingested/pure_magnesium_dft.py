"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
version v2 from materials cloud

"""

from argparse import ArgumentParser
import functools
import logging
import re
import time
from pathlib import Path
import sys

from ase.atoms import Atoms
import pymongo

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/pure_magnesium_dft_calculations_for_interatomic_potential_fitting"
)
DATASET_NAME = "pure_magnesium_DFT_PRM2020"
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "https://doi.org/10.1103/PhysRevMaterials.4.103602"
DATA_LINK = "https://doi.org/10.24435/materialscloud:8f-1s"
# OTHER_LINKS = []

AUTHORS = ["Binglun Yin", "Markus Stricker", "W. A. Curtin"]
DATASET_DESC = (
    "This dataset provides DFT (as implemented in VASP) calculations for pure "
    "magnesium. Configuration sets include bulk, generalized stacking fault "
    "energies, stable stacking fault, decohesion, relaxed surfaces, dimer, "
    "corner and rod, and vacancy configurations of Mg."
)
ELEMENTS = None
GLOB_STR = "OUTCAR"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
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
        f"{DATASET_NAME}_bulk",
        {"names": {"$regex": "1_bulk__"}},
        f"Bulk Mg configurations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_GSFE_and_stable_SF",
        {"names": {"$regex": "2_GSFE_and_stable_SF__"}},
        "Generalized stacking fault energies and stable stacking fault "
        f"Mg configurations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_decohesion_relaxed",
        {"names": {"$regex": "3_decohesion_and_relaxed_surface__"}},
        f"Decohesion and relaxed surfaces of Mg from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_dimer",
        {"names": {"$regex": "__dimer__"}},
        f"Mg dimer configurations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_corner_rod",
        {"names": {"$regex": "5_corner__"}},
        f"Corner and rod Mg configurations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_vacancy",
        {"names": {"$regex": "6_vacancy__"}},
        f"Mg configurations with varying number of atoms from {DATASET_NAME} dataset",
    ],
]


coord_re = re.compile(
    r"^\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+(?P<z>\-?\d+\.\d+)\s+(?P<fx>\-?"
    r"\d+\.\d+)\s+(?P<fy>\-?\d+\.\d+)\s+(?P<fz>\-?\d+\.\d+)"
)
param_re = re.compile(
    r"[\s+]?(?P<param>[A-Z_]+)(\s+)?=(\s+)?(?P<val>-?([\d\w\.\-]+)?\.?)"
    r"[\s;]?(?P<unit>eV)?\:?"
)
IGNORE_PARAMS = [
    "VRHFIN",
    "LEXCH",
    "EATOM",
    "TITEL",
    "LULTRA",
    "IUNSCR",
    "RPACOR",
    "POMASS",
    "RCORE",
    "RWIGS",
    "ENMAX",
    "RCLOC",
    "LCOR",
    "LPAW",
    "EAUG",
    "DEXC",
    "RMAX",
    "RAUG",
    "RDEP",
    "RDEPT",
]


def contcar_parser(fp):
    symbol_counts = dict()
    with open(fp, "r") as f:
        for i in range(5):
            _ = f.readline()
        line = f.readline()
        symbols = line.strip().split()
        counts = [int(x) for x in f.readline().strip().split()]
        symbol_counts = dict(zip(symbols, counts))
        symbols = []
        for symbol in symbol_counts:
            symbols.extend([symbol] * symbol_counts[symbol])
        return symbols


def outcar_reader(symbols, fp):
    with open(fp, "r") as f:
        configs = []
        incar = dict()
        cinput = dict()
        in_latt = False
        in_coords = False
        lattice = []
        pos = []
        forces = []
        potcars = set()
        for line in f:
            # Prelim handling
            if line.strip() == "":
                pass

            # handle lattice
            elif "direct lattice vectors" in line:
                in_latt = True
                lattice = []
                pass
            elif in_latt is True:
                # below to handle lattice numbers that run together due to minus sign
                latt = line.strip().replace("-", " -").split()
                lattice.append([float(x) for x in [latt[0], latt[1], latt[2]]])
                if len(lattice) == 3:
                    in_latt = False
            elif "POTCAR" in line:
                potcars.add(" ".join(line.strip().split()[1:]))
            elif "POSITION" in line:
                in_coords = True
                pass
            elif in_coords is True:
                if "--------" in line:
                    pass
                elif "total drift" in line:
                    in_coords = False
                    pass
                else:
                    cmatch = coord_re.search(line)
                    pos.append(
                        [float(p) for p in [cmatch["x"], cmatch["y"], cmatch["z"]]]
                    )
                    forces.append(
                        [float(p) for p in [cmatch["fx"], cmatch["fy"], cmatch["fz"]]]
                    )
            elif "FREE ENERGIE OF THE ION-ELECTRON SYSTEM" in line:
                _ = f.readline()
                _, _, _, _, energy, units = f.readline().strip().split()

                cinput["incar"] = incar
                config = Atoms(positions=pos, symbols=symbols, cell=lattice)
                config.info["input"] = cinput
                config.info["input"]["potcars"] = list(potcars)
                # config.info["outcar"] = outcar
                config.info["forces"] = forces
                config.info["energy"] = float(energy)
                config.info["name"] = f"{'__'.join(fp.parts[-5:-1])}"
                configs.append(config)
                forces = []
                pos = []
                energy = None
            else:
                pass
                # print("something went wrong")

    return configs


def get_kpoints(fp):
    with open(fp, "r") as f:
        # f.readline() # if skipping first line
        kpoints = "".join(f.readlines())
    return kpoints


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


def file_finder(fp, file_glob, count=0):
    if count > 5:
        return None
    elif file_glob in [f.name for f in fp.glob("*")]:
        return next(fp.glob(file_glob))
    else:
        count += 1
        return file_finder(fp.parent, file_glob, count)


def reader(filepath: Path):
    poscar = next(filepath.parent.glob(filepath.name.replace("OUTCAR", "CONTCAR")))
    symbols = contcar_parser(poscar)
    kpoints_file = file_finder(filepath.parent, "KPOINTS")
    kpoints = get_kpoints(kpoints_file)
    incar_file = file_finder(filepath.parent, "INCAR")
    incar = parse_incar(incar_file)

    configs = outcar_reader(fp=filepath, symbols=symbols)
    for i, config in enumerate(configs):
        # config.info["name"] = f"{'__'.join(filepath.parts[-4:-1])}_{i}"
        config.info["input"]["kpoints"] = kpoints
        config.info["input"]["incar"] = incar

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

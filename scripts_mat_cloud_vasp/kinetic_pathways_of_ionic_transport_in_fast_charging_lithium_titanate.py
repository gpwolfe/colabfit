"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

The 'stitched hops' folders contain the same data from the "neb_hop_x" folders,
but the data is stitched together to form a continuous path. These do not have
convenient INCAR or KPOINTS files, so I have used the latter folders for data.

The EELS folders contain configurations with beryllium, which is not mentioned
in the publication or readme

"""
from argparse import ArgumentParser
import functools
import logging
from pathlib import Path
import re
import sys
import time

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
    "data/kinetic_pathways_of_ionic_transport_in_fast_charging_lithium_titanate"
)
DATASET_NAME = "LiTiO_Science_2020"
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "https://doi.org/10.1126/science.aax3520"
DATA_LINK = "https://doi.org/10.24435/materialscloud:2020.0006/v1"
# OTHER_LINKS = []

AUTHORS = ["Tina Chen", "Dong-hwa Seo"]
DATASET_DESC = "Test"
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

CO_METADATA = {
    "outcar": {
        "field": "outcar",
    },
}

CSS = [
    [
        f"{DATASET_NAME}_Li4Ti5O12",
        {"names": {"$regex": "__Li4__"}},
        f"Configurations of BeLi4Ti5O12 from {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_Li5Ti5O12",
        {"names": {"$regex": "__Li5__"}},
        f"Configurations of BeLi5Ti5O12 from {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_Li6Ti5O12",
        {"names": {"$regex": "__Li6__"}},
        f"Configurations of BeLi6Ti5O12 from {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_Li7Ti5O12",
        {"names": {"$regex": "__Li7__"}},
        f"Configurations of BeLi7Ti5O12 from {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_Li4Ti5O12_add_Li",
        {"names": {"$regex": "NEB_Li4"}},
        f"Configurations of Li4Ti5O12 with an additional Li from {DATASET_NAME} "
        "dataset",
    ],
    [
        f"{DATASET_NAME}_Li5Ti5O12_add_Li",
        {"names": {"$regex": "NEB_Li5"}},
        f"Configurations of Li5Ti5O12 with an additional Li from {DATASET_NAME} "
        "dataset",
    ],
    [
        f"{DATASET_NAME}_Li7Ti5O12_minus_Li",
        {"names": {"$regex": "NEB_Li7"}},
        f"Configurations of Li7Ti5O12 with a Li vacancy from {DATASET_NAME} " "dataset",
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
        # outcar = dict()
        # in_incar = False
        in_latt = False
        in_coords = False
        lattice = []
        pos = []
        forces = []
        # settings = True
        potcars = set()
        for line in f:
            # if settings is True and "Description" in line:
            #     settings = False
            #     pass
            # Prelim handling
            if line.strip() == "":
                pass

            # handle lattice
            elif "direct lattice vectors" in line:
                in_latt = True
                lattice = []
                pass
            elif in_latt is True:
                latt = line.strip().split()
                lattice.append([float(x) for x in [latt[0], latt[1], latt[2]]])
                if len(lattice) == 3:
                    in_latt = False
            # handle incar

            # elif "INCAR" in line:
            #     in_incar = True

            #     continue

            # elif in_incar is True:
            #     if "direct lattice vectors" in line:
            #         in_incar = False
            #         pass
            elif "POTCAR" in line:
                potcars.add(" ".join(line.strip().split()[1:]))
            # else:
            #     for pmatch in param_re.finditer(
            #         line
            #     ):  # sometimes more than one param/line
            #         # param, val, unit
            #         if pmatch["unit"] is not None:
            #             outcar[pmatch["param"]] = {
            #                 "value": float(pmatch["val"]),
            #                 "units": pmatch["unit"],
            #             }
            #         else:
            #             outcar[pmatch["param"]] = pmatch["val"]
            # handle coords/nums
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
                config.info["name"] = f"{'__'.join(fp.parts[-4:-1])}"
                configs.append(config)
                forces = []
                pos = []
                energy = None
            # Check other lines for params, send to outcar or cinput
            # elif settings is True:
            #     for pmatch in param_re.finditer(line):
            #         if pmatch["param"] in IGNORE_PARAMS:
            #             pass
            #         elif pmatch["unit"] is not None:
            #             cinput[pmatch["param"]] = {
            #                 "value": float(pmatch["val"]),
            #                 "units": pmatch["unit"],
            #             }
            #         elif pmatch["unit"] is None:
            #             cinput[pmatch["param"]] = pmatch["val"]
            # elif settings is False:
            #     for pmatch in param_re.finditer(line):
            #         if pmatch["unit"] is not None:
            #             outcar[pmatch["param"]] = {
            #                 "value": float(pmatch["val"]),
            #                 "units": pmatch["unit"],
            #             }
            #         elif pmatch["unit"] is None:
            #             outcar[pmatch["param"]] = pmatch["val"]
            else:
                pass
                # print("something went wrong")

    return configs


def get_kpoints(fp):
    with open(fp, "r") as f:
        # f.readline() # if skipping if
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
            value = "".join(keyvals[1:]).strip()
            incar[key] = value
    return incar


def file_finder(fp, file_glob):
    count = 0
    if count > 5:
        return None
    elif file_glob in [f.name for f in fp.glob("*")]:
        return next(fp.glob(file_glob))
    else:
        count += 1
        return file_finder(fp.parent, file_glob)


def reader(filepath: Path):
    poscar = next(filepath.parent.glob(filepath.name.replace("OUTCAR", "POSCAR")))
    symbols = contcar_parser(poscar)
    kpoints_file = file_finder(filepath.parent, "KPOINTS")
    kpoints = get_kpoints(kpoints_file)
    incar_file = file_finder(filepath.parent, "INCAR")
    incar = parse_incar(incar_file)

    configs = outcar_reader(fp=filepath, symbols=symbols)
    for i, config in enumerate(configs):
        config.info["name"] = f"{'__'.join(filepath.parts[-4:-1])}_{i}"
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

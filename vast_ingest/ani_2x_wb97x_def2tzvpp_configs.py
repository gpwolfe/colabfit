"""
author: Gregory Wolfe

Properties
----------
potential energy

Other properties added to metadata
----------------------------------
dipole, original scf energy without the energy correction subtracted

File notes
----------
"energy" is the result of the wB97M_def2-TZVPP.scf-energies minus the
VV10.energy-corrections, as given in the source file.
coordinates | (Nc, Na, 3) | Å
species     | (Nc, Na)    | Atomic Numbers
energies    | (Nc)        | Ha
dipoles     | (Nc, 3)                | eAngstrom

'coordinates', 'dipoles', 'energies'
'mbis_atomic_charges',
'mbis_atomic_dipole_magnitudes', 'mbis_atomic_octupole_magnitudes',
'mbis_atomic_quadrupole_magnitudes', 'mbis_atomic_volumes'
'species'


Keys for the h5 file are 3-digit strings from "002" to "063", with some missing numbers,
according to the number of atoms in the configuration.
"""

import functools
import json
import logging
import sys
import time
from argparse import ArgumentParser
from functools import partial
from pathlib import Path

import h5py
import pymongo
from tqdm import tqdm

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd


DATASET_FP = Path("data/ani2x/")

AUTHORS = [
    "Kate Huddleston",
    "Roman Zubatyuk",
    "Justin Smith",
    "Adrian Roitberg",
    "Olexandr Isayev",
    "Ignacio Pickering",
    "Christian Devereux",
    "Kipton Barros",
]
ELEMENTS = None
GLOB_STR = "ANI-2x-wB97X-def2TZVPP.h5"
PI_METADATA = {
    "software": {"value": "ORCA 4.2.1"},
    "method": {"value": "DFT-wB97X"},
    "basis_set": {"value": "def2-TZVPP"},
    "input": {"! wb97x def2-tzvpp def2/j rijcosx engrad"},
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            # "reference-energy": {"field": "en_correction", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

# Configuration sets appear at bottom of script

CO_METADATA = {
    "dipole": {"field": "dipoles", "units": "electron angstrom"},
}
CO_METADATA.update(
    {
        key: {"field": key, "units": "a.u."}
        for key in [
            "mbis_atomic_charges",
            "mbis_atomic_dipole_magnitudes",
            "mbis_atomic_octupole_magnitudes",
            "mbis_atomic_quadrupole_magnitudes",
            "mbis_atomic_volumes",
        ]
    }
)

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


def ani_reader(fp, num_atoms, ds_name):
    with h5py.File(fp) as h5:
        print(num_atoms)
        properties = h5[str(num_atoms)]
        coordinates = properties["coordinates"]
        species = properties["species"]
        energies = properties["energies"]
        dipoles = properties["dipoles"]
        mbis_atomic_charges = properties["mbis_atomic_charges"]
        mbis_atomic_dipole_magnitudes = properties["mbis_atomic_dipole_magnitudes"]
        mbis_atomic_octupole_magnitudes = properties["mbis_atomic_octupole_magnitudes"]
        mbis_atomic_quadrupole_magnitudes = properties[
            "mbis_atomic_quadrupole_magnitudes"
        ]
        mbis_atomic_volumes = properties["mbis_atomic_volumes"]

        configs = []
        while True:
            for i, coord in enumerate(coordinates):
                config = AtomicConfiguration(
                    positions=coord,
                    numbers=species[i],
                )

                config.info["energy"] = energies[i]
                config.info["mbis_atomic_charges"] = mbis_atomic_charges[i]
                config.info["mbis_atomic_dipole_magnitudes"] = (
                    mbis_atomic_dipole_magnitudes[i]
                )
                config.info["mbis_atomic_octupole_magnitudes"] = (
                    mbis_atomic_octupole_magnitudes[i]
                )
                config.info["mbis_atomic_quadrupole_magnitudes"] = (
                    mbis_atomic_quadrupole_magnitudes[i]
                )
                config.info["mbis_atomic_volumes"] = mbis_atomic_volumes[i]
                config.info["dipoles"] = dipoles[i]
                config.info["name"] = f"{ds_name}__natoms_{num_atoms}__ix_{i}"
                configs.append(config)
                if len(configs) == 50000:
                    for config in configs:
                        yield config
                    configs = []
            if len(configs) > 0:
                for config in configs:
                    yield config
                break


@auto_reconnect
def read_wrapper(dbname, uri, nprocs, ds_id, ds_name, n_atoms):
    wrap_time = time.time()
    client = MongoDatabase(dbname, uri=uri, nprocs=nprocs)
    ids = []
    fp = next(DATASET_FP.rglob(GLOB_STR))
    today = time.strftime("%Y-%m-%d")
    with h5py.File(fp) as h5:
        for num_atoms in tqdm(n_atoms, desc=f"N atoms: {n_atoms[0]}-{n_atoms[-1]}"):
            if num_atoms in h5.keys():
                partial_read = partial(ani_reader, num_atoms=num_atoms, ds_name=ds_name)

                insert_time = time.time()

                configurations = load_data(
                    file_path=DATASET_FP,
                    file_format="folder",
                    name_field="name",
                    elements=ELEMENTS,
                    reader=partial_read,
                    glob_string=GLOB_STR,
                    generator=True,
                )
                ids_batch = list(
                    client.insert_data(
                        configurations=configurations,
                        ds_id=ds_id,
                        co_md_map=CO_METADATA,
                        property_map=PROPERTY_MAP,
                        generator=False,
                        verbose=False,
                    )
                )
                ids.extend(ids_batch)
                new_insert_time = time.time()
                print(f"Time to insert: {new_insert_time - insert_time}")
                insert_time = new_insert_time
                co_ids, do_ids = list(zip(*ids_batch))
                file_ds_name = ds_name.lower().replace("-", "_")
                co_id_file = Path(
                    f"{ds_id}_{file_ds_name}_co_ids/"
                    f"{ds_id}_{file_ds_name}_co_ids_{today}/"
                    f"{ds_id}_config_ids_batch_natoms_{num_atoms}.txt"
                )
                co_id_file.parent.mkdir(parents=True, exist_ok=True)
                do_id_file = Path(
                    f"{ds_id}_{file_ds_name}_do_ids/"
                    f"{ds_id}_{file_ds_name}_do_ids_{today}/"
                    f"{ds_id}_do_ids_batch_natoms_{num_atoms}.txt"
                )
                do_id_file.parent.mkdir(parents=True, exist_ok=True)
                with open(co_id_file, "a") as f:
                    f.writelines([f"{id}\n" for id in co_ids])
                with open(do_id_file, "a") as f:
                    f.writelines([f"{id}\n" for id in do_ids])

    print(f"Time to read: {time.time() - wrap_time}")
    client.close()
    return ids


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
    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="JSON file containing dataset information",
        default=None,
    )
    parser.add_argument(
        "-a", "--start", type=int, help="Start index for dataset", default=0
    )
    parser.add_argument(
        "-o", "--end", type=int, help="End index for dataset", default=64
    )

    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    with open(args.file, "r") as f:
        ds_data = json.load(f)
    ds_id = ds_data["dataset_id"]
    ds_name = ds_data["dataset_name"]
    n_atoms = [f"{x:03d}" for x in range(args.start, args.end + 1)]

    client.insert_property_definition(potential_energy_pd)

    client.close()
    ids = read_wrapper(
        dbname=client.database_name,
        uri=client.uri,
        nprocs=client.nprocs,
        ds_id=ds_id,
        ds_name=ds_name,
        n_atoms=n_atoms,
    )
    print("Num. Ids: ", len(ids))


DB_KEYS = [
    "002",
    "003",
    "004",
    "005",
    "006",
    "007",
    "008",
    "009",
    "010",
    "011",
    "012",
    "013",
    "014",
    "015",
    "016",
    "017",
    "018",
    "019",
    "020",
    "021",
    "022",
    "023",
    "024",
    "025",
    "026",
    "027",
    "028",
    "029",
    "030",
    "031",
    "032",
    "033",
    "034",
    "035",
    "036",
    "037",
    "038",
    "039",
    "040",
    "041",
    "042",
    "043",
    "044",
    "045",
    "046",
    "047",
    "048",
    "049",
    "050",
    "051",
    "052",
    "053",
    "054",
    "055",
    "056",
    "057",
    "058",
    "062",
    "063",
]

if __name__ == "__main__":
    main(sys.argv[1:])

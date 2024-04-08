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
import numpy as np
import pymongo
from tqdm import tqdm

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path("data/ANI1_release/")

ELEMENTS = None
GLOB_STR = "*.h5"
PI_METADATA = {
    "software": {"value": "Gaussian 09"},
    "method": {"value": "DFT-wB97X"},
    "basis_set": {"value": "6-31g(d)"},
    "input": {"! wb97x def2-tzvpp def2/j rijcosx engrad"},
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

# Configuration sets appear at bottom of script

CO_METADATA = {"smiles": {"field": "smiles"}}


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


def ani_reader(fp, ds_name, key_ix):
    with h5py.File(fp) as h5:
        n_heavy = int(fp.stem[-1])
        for key, val in h5.items():
            # for k1, v1 in val.items():
            k1 = list(val.keys())[key_ix]
            v1 = val[k1]

            coords = np.array(v1["coordinates"])
            # print(coords[0])
            coords_he = np.array(v1["coordinatesHE"])
            energies = np.array(v1["energies"])
            energies_he = np.array(v1["energiesHE"])
            smiles = "".join([x.decode() for x in v1["smiles"]])
            # print(smiles)
            species = [x.decode() for x in v1["species"]]
            # print(species)
            for i, coord in enumerate(coords):
                config = AtomicConfiguration(
                    positions=coord,
                    symbols=species,
                )

                config.info["energy"] = energies[i]
                config.info["smiles"] = smiles
                config.info["name"] = (
                    f"{ds_name}__n_heavy_{n_heavy}__standard_energy__{i}"
                )
                yield config
                if coords_he.shape[0] > i:

                    config = AtomicConfiguration(
                        positions=coords_he[i],
                        symbols=species,
                    )
                    config.info["energy"] = energies_he[i]
                    config.info["smiles"] = smiles
                    config.info["name"] = (
                        f"{ds_name}__n_heavy_{n_heavy}__high_energy__{i}"
                    )
                    yield config


@auto_reconnect
def read_wrapper(dbname, uri, nprocs, ds_id, ds_name, cs_ids_fp):
    wrap_time = time.time()
    client = MongoDatabase(dbname, uri=uri, nprocs=32)
    ids = []
    fps = sorted(list(DATASET_FP.rglob(GLOB_STR)))
    today = time.strftime("%Y-%m-%d")

    for fp in tqdm(fps[-1:]):

        num_heavy = int(fp.stem[-1])
        file_ds_name = ds_name.lower().replace("-", "_")
        co_id_file = Path(
            f"{ds_id}_{file_ds_name}_co_ids/"
            f"{ds_id}_{file_ds_name}_co_ids_{today}/"
            f"{ds_id}_config_ids_batch_nheavy_{num_heavy}.txt"
        )
        co_id_file.parent.mkdir(parents=True, exist_ok=True)
        do_id_file = Path(
            f"{ds_id}_{file_ds_name}_do_ids/"
            f"{ds_id}_{file_ds_name}_do_ids_{today}/"
            f"{ds_id}_do_ids_batch_natoms_{num_heavy}.txt"
        )
        do_id_file.parent.mkdir(parents=True, exist_ok=True)

        insert_time = time.time()
        for i_key in range(0, 47932):  # for number 8
            partial_read = partial(ani_reader, ds_name=ds_name, key_ix=i_key)

            configurations = load_data(
                file_path=DATASET_FP,
                file_format="folder",
                name_field="name",
                elements=ELEMENTS,
                reader=partial_read,
                glob_string=fp.name,
                generator=True,
            )
            ids_batch = list(
                client.insert_data(
                    configurations=configurations,
                    ds_id=ds_id,
                    co_md_map=CO_METADATA,
                    property_map=PROPERTY_MAP,
                    generator=False,
                    verbose=True,
                )
            )
            ids.extend(ids_batch)
            new_insert_time = time.time()
            print(f"Time to insert: {new_insert_time - insert_time}")
            insert_time = new_insert_time
            co_ids, do_ids = list(zip(*ids_batch))
            with open(co_id_file, "a") as f:
                f.writelines([f"{id}\n" for id in co_ids])
            with open(do_id_file, "a") as f:
                f.writelines([f"{id}\n" for id in do_ids])

        print(f"Time to read: {time.time() - wrap_time}")
        css = (
            (
                f"{ds_name}__num_heavy_{num_heavy}__standard_energy",
                {"names": {"$regex": "standard_energy"}},
                f"Configurations from {ds_name} with {num_heavy} heavy atoms "
                "and standard energy (that is, not high energy)",
            ),
            (
                f"{ds_name}__num_heavy_{num_heavy}__high_energy",
                {"names": {"$regex": "high_energy"}},
                f"Configurations from {ds_name} with {num_heavy} heavy atoms "
                "and high energy",
            ),
        )
        cs_ids = []
        for cs_name, cs_query, cs_desc in css:
            if cs_name in [
                "ANI-1__num_heavy_1__high_energy",  # 0 configs in sets
                "ANI-1__num_heavy_3__high_energy",
            ]:
                continue
            else:
                cs_ids.append(
                    client.query_and_insert_configuration_set(
                        co_hashes=co_ids,
                        query=cs_query,
                        ds_id=ds_id,
                        name=cs_name,
                        description=cs_desc,
                    )
                )
        with open(cs_ids_fp, "a") as f:
            f.writelines([f"{id}\n" for id in cs_ids])
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

    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    with open(args.file, "r") as f:
        ds_data = json.load(f)
    ds_id = ds_data["dataset_id"]
    ds_name = ds_data["dataset_name"]

    client.insert_property_definition(potential_energy_pd)

    client.close()
    ids = read_wrapper(
        dbname=client.database_name,
        uri=client.uri,
        nprocs=client.nprocs,
        ds_id=ds_id,
        ds_name=ds_name,
        cs_ids_fp=ds_data["cs_ids_fp"],
    )
    print("Num. Ids: ", len(ids))


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author: gpwolfe

File notes
----------
There are several different files and file types
trajectories appear to be contained in extxyz.xz compressed files in
the subdir is2res_train_trajectories
There is some data in the form of a matching filestem-id followed by a float in
a file called 'system.txt'. This appears to be close to the potential energy values
in the corresponding extxyz.xz file.
Files with different "random123456" ids are contained in the subdir uc
These are plain extxyz files
There is a set of metadata for the latter contained in the file 'oc20_data_mapping.pkl'

The reconnect functool function comes from:
https://gist.github.com/anthonywu/1696591#file-graceful_auto_reconnect-py
as a way to handle a disconnect/connection reset by peer (hopfully)



2023.4.1: trying batching without creating and closing the client for each batch. Ids
are being saved to {ds-id}_config_ids.txt and {ds-id}_do_id.txt. Next step would be to
do the dataset creation.

"""

from argparse import ArgumentParser
from ase.io import read
from colabfit import ATOMS_LABELS_FIELD, ATOMS_NAME_FIELD
from colabfit.tools.converters import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
    free_energy_pd,
)
import functools
import itertools
import logging
import multiprocessing
import numpy as np
from pathlib import Path
import sys
import time
from tqdm import tqdm

# import aiohttp
import asyncio
import pymongo


BATCH_SIZE = 512
BATCH_SIZE = 2
DATASET_FP = Path("/vast/gw2338/is2res_train_trajectories")  # Greene
DATASET_FP = Path("is2res_train_trajectories")  # local


today = f"{time.strftime('%Y')}_{time.strftime('%m')}_{time.strftime('%d')}"
AUTHORS = [
    "Lowik Chanussot",
    "Abhishek Das",
    "Siddharth Goyal",
    "Thibaut Lavril",
    "Muhammed Shuaibi",
    "Morgane Riviere",
    "Kevin Tran",
    "Javier Heras-Domingo",
    "Caleb Ho",
    "Weihua Hu",
    "Aini Palizhati",
    "Anuroop Sriram",
    "Brandon Wood",
    "Junwoong Yoon",
    "Devi Parikh",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
]
PUBLICATION = "https://arxiv.org/abs/2010.09990"
DATA_LINK = "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md"
LINKS = [
    "https://arxiv.org/abs/2010.09990",
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md",
]
DS_DESC = (
    "Configurations for the initial structure to relaxed energy "
    "(IS2RE) and initial structure to relaxed structure (IS2RS) tasks of "
    'Open Catalyst 2020 (OC20). Dataset corresponds to the "All IS2RE/S training '
    'data split under the "Relaxation Trajectories" '
    "section of the Open Catalyst Project GitHub page."
)
DATASET = "OC20_IS2RES_train"
PKL_FP = DATASET_FP / "oc20_data_mapping.pkl"
GLOB_STR = "*.extxyz"
NAME_FIELD = "name"
LABELS_FIELD = "labels"

ID_META_MAP = np.load(PKL_FP, allow_pickle=True)


# async def fetch_data():
#     url = "http://10.32.250.13:30007"
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as _:
#             pass


# async def poke_nodeport_and_sleep():
#     while True:
#         await fetch_data()
#         await asyncio.sleep(240)


# async def run_poke_and_sleep():
#     await poke_nodeport_and_sleep()


with open(DATASET_FP / "system.txt", "r") as f:
    ref_text = [x.strip().split(",") for x in f.readlines()]
    REF_E_DICT = {k: float(v) for k, v in ref_text}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "ref_energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "reference_energy": {"field": "ref_energy"},
            },
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "ref_energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
            },
        }
    ],
}
co_md_map = {
    "bulk_id": {"field": "bulk_id"},
    "bulk_mpid": {"field": "bulk_mpid"},
    "ads_id": {"field": "ads_id"},
    "bulk_symbols": {"field": "bulk_symbols"},
    "ads_symbols": {"field": "ads_symbols"},
    "miller_index": {"field": "miller_index"},
    "shift": {"field": "shift"},
    "adsorption_site": {"field": "adsorption_site"},
    "oc_class": {"field": "class"},
    "oc_anomaly": {"field": "anomaly"},
    "frame": {"field": "frame"},
    "oc-id": {"field": "oc_id"},
}


def reader(filepath):
    fp_stem = filepath.stem
    configs = []
    ase_configs = read(filepath, index=":")

    for i, ase_config in enumerate(ase_configs):
        config = AtomicConfiguration().from_ase(ase_config)
        # positions=ase_config.positions,
        # numbers=ase_config.numbers,
        # pbc=ase_config.pbc,
        # cell=ase_config.cell,
        # )
        config.info = ase_config.info
        config.info["forces"] = ase_config.arrays["forces"]
        config.info["ref_energy"] = REF_E_DICT[fp_stem]
        config.info["system_id"] = fp_stem
        config.info["name"] = f"{filepath.parts[-3]}_{fp_stem}_{i}"
        id_meta = ID_META_MAP.get(fp_stem)
        if id_meta:
            config.info.update(id_meta)
        configs.append(config)

    return configs


def read_for_pool(filepath):
    configurations = []
    new = reader(filepath)
    for i, atoms in enumerate(new):
        if NAME_FIELD in atoms.info:
            atoms.info[ATOMS_NAME_FIELD] = [atoms.info[NAME_FIELD]]
        else:
            raise RuntimeError(
                f"Field {NAME_FIELD} not in atoms.info for index "
                f"{i} in {filepath}. Set `name_field=None` "
                "to use `default_name`."
            )

        if LABELS_FIELD not in atoms.info:
            atoms.info[ATOMS_LABELS_FIELD] = set()
        else:
            atoms.info[ATOMS_LABELS_FIELD] = set(atoms.info[LABELS_FIELD])
        configurations.append(atoms)

    return configurations


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


async def get_configs(ds_id, client_name, client_uri, nprocs, batch):
    # ids = []
    batch_num, filepaths = batch
    pool = multiprocessing.Pool(nprocs)
    configurations = list(
        itertools.chain.from_iterable(pool.map(read_for_pool, filepaths))
    )
    client = MongoDatabase(database_name=client_name, uri=client_uri, nprocs=nprocs)
    ids_batch = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )
    co_ids, do_ids = list(zip(*ids_batch))
    co_id_file = Path(
        f"{ds_id}_co_ids_{today}/{ds_id}_config_ids_batch_{batch_num}.txt"
    )
    co_id_file.parent.mkdir(parents=True, exist_ok=True)
    do_id_file = Path(f"{ds_id}_do_ids_{today}/{ds_id}_do_ids_batch_{batch_num}.txt")
    do_id_file.parent.mkdir(parents=True, exist_ok=True)
    with open(co_id_file, "a") as f:
        f.writelines([f"{id}\n" for id in co_ids])
    with open(do_id_file, "a") as f:
        f.writelines([f"{id}\n" for id in do_ids])
    # ids.extend(ids_batch)
    # client.close()
    return do_ids


MAX_AUTO_RECONNECT_ATTEMPTS = 100


@auto_reconnect
async def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    parser.add_argument("-r", "--port", type=int, help="Target port for MongoDB")

    args = parser.parse_args(argv)
    nprocs = args.nprocs
    client = MongoDatabase(
        args.db_name, nprocs=nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    ds_id = generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    fps = sorted(list(DATASET_FP.rglob(GLOB_STR)))
    n_batches = len(fps) // BATCH_SIZE
    leftover = len(fps) % BATCH_SIZE
    indices = [((b * BATCH_SIZE, (b + 1) * BATCH_SIZE)) for b in range(n_batches)]
    if leftover:
        indices.append((BATCH_SIZE * n_batches, len(fps)))
    get_configs_partial = functools.partial(
        get_configs, ds_id, client.database_name, client.uri, nprocs
    )
    batches = []
    for batch_num, batch in tqdm(enumerate(indices)):
        beg, end = batch
        filepaths = fps[beg:end]
        batches.append((batch_num, filepaths))
    # get_configs_partial(batches[0])
    tasks = [asyncio.create_task(get_configs_partial(batch)) for batch in batches]
    print("after tasks")
    _ = await asyncio.gather(*tasks)

    # ids = get_configs(ds_id, args)

    # all_co_ids, all_do_ids = list(zip(*ids))

    # client.insert_dataset(
    #     do_hashes=all_do_ids,
    #     ds_id=ds_id,
    #     name=DATASET,
    #     authors=AUTHORS,
    #     links=[PUBLICATION, DATA_LINK],
    #     description=DS_DESC,
    #     verbose=False,
    #     data_license="https://creativecommons.org/licenses/by/4.0/",
    # )


async def submain(args):
    asyncio.create_task(main(args))
    # await run_poke_and_sleep()


if __name__ == "__main__":
    args = sys.argv[1:]
    asyncio.run(main(args))

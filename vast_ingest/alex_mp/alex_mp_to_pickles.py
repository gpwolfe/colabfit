import json
import os
import pickle as pkl
import sys
from pathlib import Path

import lmdb
import torch
from ase.atoms import Atoms
from colabfit.tools.vast.database import batched
from dotenv import load_dotenv
from optimade.client import OptimadeClient
from tqdm import tqdm

load_dotenv()
mp_key = os.getenv("MP_KEY")


def from_lmdb(
    env: lmdb.Environment,
    key: str,
    split_name: str,
):
    """
    Read ase.atoms.Atoms object from lmdb.

    Args:
        env: lmdb.Environment object.
        key: key to read the configuration from the lmdb.
    """
    with env.begin() as txn:
        data = txn.get(key.encode())
        if data is None:
            raise ValueError(f"Key {key} not found in the lmdb.")
        lmdb_config = pkl.loads(data)
        cell = lmdb_config.pop("cell", None)
        numbers = lmdb_config.pop("atomic_numbers", None)
        coords = lmdb_config.pop("pos", None)
        if coords is None:
            raise ValueError(f"Key {key} has no coordinates in the lmdb.")
        pbc = lmdb_config.pop("pbc", None)
        identifier = key
        property_keys = lmdb_config.keys()
        property_dict = {}
        if isinstance(property_keys, str):
            property_keys = (property_keys,)
        for prop in property_keys:
            property_dict[prop] = lmdb_config.get(prop, None)
            if (
                property_dict[prop] is not None
                and torch.is_tensor(property_dict[prop])
                and property_dict[prop].is_floating_point()
            ):
                property_dict[prop] = property_dict[prop]
        # print(property_dict)
        pbc = [bool(i) for i in pbc]
        config = Atoms(
            numbers=numbers,
            cell=cell,
            positions=coords,
            pbc=pbc,
        )
        config.info["alex_mp_split"] = split_name
        config.info["alex_mp_file_key"] = identifier
        config.info.update(property_dict)
        config.info["id"] = config.info.pop("ids", None)
        # config.info |= {"ds_idx": ds_idx}
    return config


def read_lmdb(
    lmdb_path: str,
):
    """
    Read ase.atoms.Atoms object from lmdb.

    Args:
        lmdb_path: Path to the lmdb database.
        key: key to read the configuration from the lmdb.
        split_name: name of the split (e.g. 'train', 'val', 'test').
        property_keys: list of property keys to read from the lmdb.
    """
    if isinstance(lmdb_path, str):
        lmdb_path = Path(lmdb_path)
    env = lmdb.open(
        str(lmdb_path),
        readonly=True,
        lock=False,
        subdir=False,
        readahead=False,
        meminit=False,
    )

    try:
        keys = set()
        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                keys.add(key.decode())
        for key in keys:
            config = from_lmdb(
                env=env,
                key=key,
                split_name=lmdb_path.stem,
            )
            yield config
    finally:
        env.close()


def get_alex_batch(alex_id_batch):
    alex_missing = set()
    pbe_dict = {}
    pbesol_dict = {}
    for sub_batch in batched(alex_id_batch, 100):
        alex_client = OptimadeClient(
            [
                "https://alexandria.icams.rub.de/pbe",
                "https://alexandria.icams.rub.de/pbesol",
            ],
        )
        alex_id_filter = 'id="' + '" OR id="'.join(sub_batch) + '"'
        alex_response = alex_client.get(filter=alex_id_filter)
        key1 = list(alex_response["structures"].keys())[0]
        pbe = list(alex_response["structures"][key1].keys())[0]
        pbesol = list(alex_response["structures"][key1].keys())[1]
        print(pbesol)
        print(key1)
        print(alex_response["structures"].keys())
        print(alex_response["structures"][key1].keys())
        pbesol_ids = [
            x["id"] for x in alex_response["structures"][key1][pbesol]["data"]
        ]
        pbe_ids = [
            x["id"]
            for x in alex_response["structures"][key1][pbe]["data"]
            if x["id"] not in pbesol_ids
        ]
        alex_missing_batch = set(alex_id_batch) - (set(pbe_ids + pbesol_ids))
        pbe_dict_batch = {
            x["id"]: {
                "energy": x["attributes"]["_alexandria_energy"],
                "forces": x["attributes"]["_alexandria_forces"],
                "stress": x["attributes"]["_alexandria_stress_tensor"],
            }
            for x in alex_response["structures"][key1][pbe]["data"]
        }
        pbesol_dict_batch = {
            x["id"]: {
                "energy": x["attributes"]["_alexandria_energy"],
                "forces": x["attributes"]["_alexandria_forces"],
                "stress": x["attributes"]["_alexandria_stress_tensor"],
            }
            for x in alex_response["structures"][key1][pbesol]["data"]
        }
        pbe_dict.update(pbe_dict_batch)
        pbesol_dict.update(pbesol_dict_batch)
        alex_missing.update(alex_missing_batch)
    return pbe_dict, pbesol_dict, alex_missing


def main(split):
    config_gen = read_lmdb(
        f"/home/gpwolfe/colabfit/vast_ingest/alex_mp/data/{split}.lmdb"
    )
    # with open(f"material_to_task_map.csv") as f:
    #     mp_map = {}
    #     for line in f:
    #         mat, task, method = line.strip().split(",")
    #         if not mp_map.get(mat):
    #             mp_map[mat] = {}
    #             mp_map[mat][task] = method
    with open("materials_project_properties.json") as f:
        mp_tasks = json.load(f)
    for batch_num, batch in enumerate(batched(config_gen, 10000)):
        configs = []
        print(batch[0].info)
        alex_ids = [
            conf.info["id"].replace("alex<", "")[:-1]
            for conf in batch
            if "alex" in conf.info["id"]
        ]
        pbe_dict, pbesol_dict, alex_missing = get_alex_batch(alex_ids)
        for i, conf in tqdm(enumerate(batch)):
            if "alex" in conf.info["id"]:
                alex_id = conf.info["id"].replace("alex<", "")[:-1]
                conf.info["alexandria_id"] = alex_id
                if alex_id in pbe_dict:
                    conf.info.update(pbe_dict[alex_id])
                    conf.info["alexandria_source"] = "pbe"
                elif alex_id in pbesol_dict:
                    conf.info.update(pbesol_dict[alex_id])
                    conf.info["alexandria_source"] = "pbesol"
                else:
                    conf.info["missing_alexandria_object"] = True
            if "mp-" in conf.info["id"]:
                mp_id = conf.info["id"]
                conf.info["materials_project_id"] = mp_id
                try:
                    task = mp_tasks[mp_id]
                    conf.info.update(task)
                except KeyError:
                    conf.info["missing_mp_task"] = True
            configs.append(conf)
        with open(
            f"alex_mp_pickles3/materials_project_ids_{split}_{batch_num}.pickle", "wb"
        ) as f:
            pkl.dump(configs, f)


if __name__ == "__main__":
    split = sys.argv[1]
    main(split)

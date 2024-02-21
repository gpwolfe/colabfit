from argparse import ArgumentParser

# from functools import partial
from pathlib import Path
from colabfit.tools.database import MongoDatabase

# from multiprocessing import Pool

DATASET_NAME = "ANI-2x-wB97X-631Gd"


def insert_cs(ds_id, filepath):
    try:
        natoms = int(filepath.stem.split("_")[-1])
        with open(filepath, "r") as f:
            co_ids = [line.strip().replace("CO_", "") for line in f.readlines()]
            print(co_ids)
        client = MongoDatabase(
            # database_name="ani12",
            # uri="mongodb://localhost:27017",
            database_name="cf-update-2023-11-30",
            uri="mongodb://10.32.250.13:30007",
        )

        name = f"{DATASET_NAME}_num_atoms_{natoms}"
        # query = ({"names": {"$regex": f"natoms_{natoms:03d}__"}},)
        desc = f"Configurations with {natoms} atoms from {DATASET_NAME} dataset"

        cs_id = client.query_and_insert_configuration_set(
            co_hashes=co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=None,
        )
        with open(f"{ds_id}_cs_ids.txt", "a") as f:
            f.write(f"{cs_id}\n")

        return cs_id
    except Exception as e:
        with open(f"{ds_id}_error_log.txt", "a") as f:
            f.write(f"{ds_id} {filepath} {e}\n")


def main(co_dir, ds_id):
    fps = sorted(list(co_dir.rglob(f"{ds_id}*.txt")))
    for fp in fps:
        print(fp)
        insert_cs(ds_id, fp)
    # part_insert_cs = partial(insert_cs, ds_id)
    # p = Pool(16)
    # p.map(part_insert_cs, fps)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--co_dir", type=Path)
    parser.add_argument("--ds_id", type=str)
    args = parser.parse_args()
    main(args.co_dir, args.ds_id)

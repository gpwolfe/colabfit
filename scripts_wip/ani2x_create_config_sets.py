from argparse import ArgumentParser

# from functools import partial
from pathlib import Path

from colabfit.tools.database import MongoDatabase

# from multiprocessing import Pool


def insert_cs(filepath, ds_id, ds_name):
    try:
        natoms = int(filepath.stem.split("_")[-1])
        with open(filepath, "r") as f:
            co_ids = [line.strip().replace("CO_", "") for line in f.readlines()]
            print(f"number of COs in natoms batch {natoms}: {len(co_ids)}")
        client = MongoDatabase(
            database_name="ani2x_wb97x_631gd-test2",
            # uri="mongodb://localhost:30007",
            # database_name="cf-update-2023-11-30",
            uri="mongodb://10.32.250.13:30007",
        )

        name = f"{ds_name}_num_atoms_{natoms}"
        print(f"CS name: {name}")
        # query = ({"names": {"$regex": f"natoms_{natoms:03d}__"}},)
        desc = f"Configurations with {natoms} atoms from {ds_name} dataset"

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


def main(co_dir, ds_id, ds_name):
    fps = sorted(list(co_dir.rglob(f"{ds_id}*.txt")))
    for fp in fps:
        print(fp)
        insert_cs(fp, ds_id, ds_name)
    # part_insert_cs = partial(insert_cs, ds_id)
    # p = Pool(16)
    # p.map(part_insert_cs, fps)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--co_dir", type=Path, required=True)
    parser.add_argument("--ds_id", type=str, required=True)
    parser.add_argument("--ds_name", type=str, required=True)
    args = parser.parse_args()
    main(args.co_dir, args.ds_id, args.ds_name)

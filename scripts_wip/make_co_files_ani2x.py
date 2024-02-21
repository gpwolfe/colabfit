from tqdm import tqdm
from pymongo import MongoClient
from pathlib import Path
from multiprocessing import Pool


fps = sorted(
    [
        path
        for path in Path("DS_wxf8f3t3abul_0_do_ids_2024-02-17").glob(
            "DS_wxf8f3t3abul_0_do_ids_batch_natoms_*.txt"
        )
        if (
            int(path.stem.split("_")[-1])
            > 10
            # and int(path.stem.split("_")[-1]) < 37
        )
    ]
)


def get_co_id(fp):
    print(fp)
    client = MongoClient(host="mongodb://10.32.250.13:30007")
    db = client["cf-update-2023-11-30"]

    coll = db["data_objects"]
    # configs = db["configurations"]
    with open(fp, "r") as f:
        do_ids = f.read().splitlines()
    new_fp = Path("DS_wxf8f3t3abul_0_co_ids_2024-02-17") / (
        fp.stem.replace("do_ids", "co_ids") + ".txt"
    )
    new_fp.parent.mkdir(exist_ok=True, parents=True)

    with open(new_fp, "w") as f:
        for dohash in tqdm(do_ids):
            dobj = coll.find_one({"hash": dohash}, {"relationships.configuration": 1})
            co_id = dobj["relationships"][0]["configuration"]
            f.write(f"{co_id}\n")


if __name__ == "__main__":
    with Pool(16) as p:
        p.map(get_co_id, fps)
    # for fp in fps:
    #     get_co_id(fp)

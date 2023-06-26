from collections import defaultdict
from pymongo import MongoClient
from tqdm import tqdm
from multiprocessing import Pool

# client = MongoClient("mongodb://10.0.52.71:27017/")
# db = client["colabfit-2023-5-16"]

client = MongoClient("mongodb://localhost:5000/")
db = client["colabfit-2023-5-16"]

# client = MongoClient("mongodb://localhost:27017/")
# db = client["mp"]

dss = sorted([ds["colabfit-id"] for ds in db.datasets.find({}, {"colabfit-id": 1})])
finished = [
    "DS_p6m0q7c6dd64_0",
]


def get_pi_do_md(ds_id):
    pi_ids = defaultdict(list)
    dos = db.data_objects.find({"relationships.datasets": ds_id}, {"colabfit-id": 1})
    for do in tqdm(dos):
        pis = db.property_instances.find(
            {"relationships.data_objects": do["colabfit-id"]},
            {
                "colabfit-id": 1,
                "relationships.data_objects": 1,
                "relationships.metadata": 1,
            },
        )

        for pi in pis:
            pi_ids[pi["colabfit-id"]] = (
                pi["relationships"]["data_objects"],
                pi["relationships"]["metadata"],
            )
    return pi_ids


def get_pi_data(ds_id):
    name = db.datasets.find_one({"colabfit-id": ds_id}, {"name": 1})
    name = name["name"]
    print(name)
    pis = get_pi_do_md(ds_id)
    mds_over_one = sum([len(val[1]) > 1 for k, val in pis.items()])
    unequal_md_do = sum(
        [(len(val[1]) > 1 and len(val[0]) != len(val[1])) for k, val in pis.items()]
    )

    with open(f"{name}_pi_data.txt", "w") as f:
        f.write(f"{name}, {ds_id}\n")
        f.write(f"mds_over_one, {mds_over_one}\n")
        f.write(f"unequal_md_do, {unequal_md_do}\n")
        f.writelines([f"{key}, {val}\n" for key, val in pis.items()])
    return pis


if __name__ == "__main__":
    dss = sorted([ds["colabfit-id"] for ds in db.datasets.find({}, {"colabfit-id": 1})])
    finished = [
        "DS_p6m0q7c6dd64_0",
    ]
    with Pool(10) as p:
        p.map(get_pi_data, [ds for ds in dss if ds not in finished])

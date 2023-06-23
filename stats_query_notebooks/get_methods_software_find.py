from collections import defaultdict
from pymongo import MongoClient
from tqdm import tqdm

# client = MongoClient("mongodb://10.0.52.71:27017/")
# db = client["colabfit-2023-5-16"]

client = MongoClient("mongodb://localhost:27017/")
db = client["mp"]


def get_soft_meth(
    batch_size,
    last,
    typ,
    methods: defaultdict(int),
    software: defaultdict(int),
    oc_methods: defaultdict(int),
    oc_software: defaultdict(int),
):
    pi_hash = None
    found = (
        db.property_instances.find(
            {"hash": {"$gt": last}, "type": typ},
            {"hash": 1, "relationships.data_objects": 1, "relationships.metadata": 1},
        )
        .sort("hash", 1)
        .limit(batch_size)
    )
    for pi in found:
        pi_hash = pi["hash"]
        mds = list(
            db.metadata.find(
                {"colabfit-id": {"$in": pi["relationships"]["metadata"]}},
                {"method": 1, "software": 1},
            )
        )
        dos = list(
            db.data_objects.find(
                {
                    "colabfit-id": {"$in": pi["relationships"]["data_objects"]},
                    # "relationships.datasets": {"$ne": "DS_ifdjgm9le1fd_0"},
                },
                {"relationships.datasets": 1},
            )
        )
        # Equal length
        if len(mds) == len(dos):
            for i, do in enumerate(dos):
                meth = mds[i].get("method", {}).get("source-value")
                if meth is None or len(meth) == 0:
                    methods["None"] += 1
                else:
                    methods[meth] += 1
                soft = mds[i].get("software", {}).get("source-value")
                if soft is None or len(soft) == 0:
                    software["None"] += 1
                else:
                    software[soft] += 1
                if "DS_ifdjgm9le1fd_0" in do["relationships"]["datasets"]:
                    oc_methods[meth] += 1
                    oc_software[soft] += 1
        # One MD -> multiple DOs
        elif len(mds) == 1 and len(dos) > 1:
            for i, do in enumerate(dos):
                meth = mds[0].get("method", {}).get("source-value")
                if meth is None or len(meth) == 0:
                    methods["None"] += 1
                else:
                    methods[meth] += 1
                soft = mds[0].get("software", {}).get("source-value")
                if soft is None or len(soft) == 0:
                    software["None"] += 1
                else:
                    software[soft] += 1
                if "DS_ifdjgm9le1fd_0" in do["relationships"]["datasets"]:
                    oc_methods[meth] += 1
                    oc_software[soft] += 1
        else:
            methods["unequal_do_md_lens"] += 1
            software["unequal_do_md_lens"] += 1
            if "DS_ifdjgm9le1fd_0" in do["relationships"]["datasets"]:
                oc_methods["unequal_do_md_lens"] += 1
                oc_software["unequal_do_md_lens"] += 1
    return pi_hash, methods, software, oc_methods, oc_software


def main(typ):
    methods = defaultdict(int)
    software = defaultdict(int)
    oc_methods = defaultdict(int)
    oc_software = defaultdict(int)

    n_pis = db.property_instances.estimated_document_count()
    b_size = 100000
    n_batches = n_pis // b_size
    if n_batches == 0:
        n_batches = 1
    last = "0"

    for batch in tqdm(range(n_batches)):
        last, methods, software, oc_methods, oc_software = get_soft_meth(
            b_size, last, typ, methods, software, oc_methods, oc_software
        )
        if last is None:
            break

    with open(f"methods_w_oc_{typ}_find.txt", "w") as f:
        f.writelines(sorted([f"{key}, {val}\n" for key, val in methods.items()]))
    with open(f"methods_oc_only_{typ}_find.txt", "w") as f:
        f.writelines(sorted([f"{key}, {val}\n" for key, val in oc_methods.items()]))
    with open(f"software_w_oc_{typ}_find.txt", "w") as f:
        f.writelines(sorted([f"{key}, {val}\n" for key, val in software.items()]))
    with open(f"software_oc_only_{typ}_find.txt", "w") as f:
        f.writelines(sorted([f"{key}, {val}\n" for key, val in oc_software.items()]))


if __name__ == "__main__":
    for typ in [
        # "potential-energy",
        "free-energy",
        # "formation-energy",
        # "atomization-energy",
    ]:
        print(typ)
        main(typ)

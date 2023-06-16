from collections import defaultdict
from pymongo import MongoClient
from tqdm import tqdm

client = MongoClient("mongodb://localhost:5000/")
db = client["colabfit-2023-5-16"]


def get_soft_meth(batch_size, skip, type):
    piped = db.property_instances.aggregate(
        [
            {"$match": {"type": type}},
            # {"$sort": {"_id": 1}},
            {"$skip": skip},
            {"$limit": batch_size},
            {"$unwind": "$relationships.metadata"},
            {
                "$lookup": {
                    "from": "metadata",
                    "localField": "relationships.metadata",
                    "foreignField": "colabfit-id",
                    "as": "do_data",
                }
            },
            {
                "$lookup": {
                    "from": "data_objects",
                    "localField": "do_data.colabfit-id",
                    "foreignField": "colabfit-id",
                    "as": "data_object",
                }
            },
            {
                "$match": {
                    "data_object.relationships.datasets": {"$ne": ["DS_ifdjgm9le1fd_0"]}
                }
            },
            {
                "$group": {
                    "_id": "$colabfit-id",
                    "md_ids": {"$push": "$relationships.metadata"},
                    "do_ids": {"$push": "$relationships.data_objects"},
                    "method": {"$push": "$do_data.method.source-value"},
                    "software": {"$push": "$do_data.software.source-value"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "md_ids": {"$size": "$md_ids"},
                    "do_ids": {"$size": "$do_ids"},
                    "method": "$method",
                    "software": "$software",
                }
            },
        ]
    )
    return piped


def update_ms(ms_data, soft_dict, meth_dict):
    for data in ms_data:
        meth = data["method"][0]
        soft = data["software"][0]
        do_len = data["do_ids"]
        md_len = data["md_ids"]
        # If the number of md-ids == num of do-ids
        if do_len == md_len:
            if len(meth) == 0:
                meth_dict["None"] += do_len
            else:
                if len(meth) == 1:
                    meth_dict[meth[0]] += do_len
                elif len(meth) == do_len:
                    for m in meth:
                        meth_dict[m] += 1
                else:
                    meth_dict["unequal_meth_domd"] += do_len
            if len(soft) == 0:
                soft_dict["None"] += do_len
            else:
                if len(soft) == 1:
                    soft_dict[soft[0]] += do_len
                elif len(soft) == do_len:
                    for s in soft:
                        soft_dict[s] += 1
                else:
                    soft_dict["unequal_soft_domd"] += do_len
        else:
            soft_dict["unequal_do_md"] += 1
            meth_dict["unequal_do_md"] += 1


def main(typ):
    methods = defaultdict(int)
    software = defaultdict(int)
    n_pis = db.property_instances.estimated_document_count()
    b_size = 500000
    n_batches = n_pis // b_size
    remain = n_pis % b_size
    for batch in tqdm(range(n_batches)):
        skip = batch * b_size
        data = get_soft_meth(b_size, skip, typ)
        update_ms(data, software, methods)

    skip = n_batches * b_size
    data = get_soft_meth(remain, skip, typ)
    update_ms(data, software, methods)

    with open(f"software_no_oc_{typ}.txt", "a") as f:
        f.write(str(software))
        print(software)
    with open(f"methods_no_oc_{typ}.txt", "a") as f:
        f.write(str(methods))
        print(methods)


if __name__ == "__main__":
    for typ in [
        "potential-energy",
        "atomic-forces",
        "free-energy",
        "formation-energy",
        "band-gap",
        "cauchy-stress",
        "atomization-energy",
    ]:
        main(typ)

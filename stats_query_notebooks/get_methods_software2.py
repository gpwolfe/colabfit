# from bson.objectid import ObjectId
from collections import defaultdict

# from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm


# client = MongoClient("mongodb://localhost:5000/")
# db = client["colabfit-2023-5-16"]

# Local testing
client = MongoClient("mongodb://localhost:27017/")
db = client["mp"]


def get_soft_meth(
    batch_size,
    last,
    typ,
):
    piped = db.property_instances.aggregate(
        [
            {"$match": {"type": typ}},
            {"$sort": {"hash": 1}},
            # Include only PI objects after the last object from previous batch
            {"$match": {"hash": {"$gt": last}}},
            {"$limit": batch_size},
            # Split on each MD-id and join MDs and DOs
            {"$unwind": "$relationships.metadata"},
            {
                "$lookup": {
                    "from": "metadata",
                    "localField": "relationships.metadata",
                    "foreignField": "colabfit-id",
                    "as": "md_data",
                }
            },
            {
                "$lookup": {
                    "from": "data_objects",
                    "localField": "relationships.data_objects",
                    "foreignField": "colabfit-id",
                    "as": "data_object",
                }
            },
            # Match only returned docs that don't point to OC20
            {
                "$match": {
                    "data_object.relationships.datasets": {"$ne": "DS_ifdjgm9le1fd_0"}
                    # "data_object.relationships.datasets": {"$ne": "DS_6b94omk25jdj_0"}
                }
            },
            # Regroup objects based on original hash (PI hash)
            {
                "$group": {
                    "_id": "$hash",
                    "md_ids": {"$push": "$relationships.metadata"},
                    "do_ids": {"$push": "$relationships.data_objects"},
                    "method": {"$push": "$md_data.method.source-value"},
                    "software": {"$push": "$md_data.software.source-value"},
                }
            },
            # _id should be hash at this point
            {
                "$project": {
                    "_id": "$_id",
                    "md_ids": {"$size": "$md_ids"},
                    "do_ids": {"$size": "$do_ids"},
                    "method": "$method",
                    "software": "$software",
                }
            },
            # {"$sort": {"_id": 1}},
        ]
    )
    return piped


def update_ms(ms_data, soft_dict, meth_dict):
    hashes = []
    for data in ms_data:
        id = data.get("_id")
        # print(id)
        # if id is None:
        #     return None
        # else:
        hashes.append(id)
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
    if len(hashes) == 0:
        return None
    return max(hashes)


def main(typ):
    n_pis = db.property_instances.estimated_document_count()
    b_size = 1000
    n_batches = n_pis // b_size
    if n_batches < 1:
        n_batches = 1
    methods = defaultdict(int)
    software = defaultdict(int)

    # dt = datetime(2010, 1, 1)
    # last = ObjectId.from_datetime(dt)
    last = "0"

    for batch in tqdm(range(n_batches)):
        data = get_soft_meth(b_size, last, typ)
        last = update_ms(data, software, methods)
        # print(last)
        if last is None:
            break

    with open(f"software_test_no_oc_{typ}.txt", "a") as f:
        f.write(str(software))
        print(software)
    with open(f"methods_test_no_oc_{typ}.txt", "a") as f:
        f.write(str(methods))
        print(methods)


if __name__ == "__main__":
    for typ in [
        "potential-energy",
        "atomic-forces",
        "free-energy",
        # "formation-energy",
        # "band-gap",
        # "cauchy-stress",
        # "atomization-energy",
    ]:
        print(typ)
        main(typ)

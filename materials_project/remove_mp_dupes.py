from pymongo import MongoClient

client = MongoClient("mongodb://localhost:5000/")
db = client["colabfit-2023-5-16"]
ds_id = "DS_2wiwpg3oyupj_0"

while True:
    dos = db.data_objects.find(
        {"relationships.datasets": [ds_id]}, {"colabfit-id": 1}
    ).limit(500)
    do_ids = list(do["colabfit-id"] for do in dos)
    print(len(do_ids))
    if len(do_ids) == 0:
        break

    filter = {"relationships.data_objects": {"$elemMatch": {"$in": do_ids}}}

    db.property_instances.update_many(
        filter=filter,
        update={"$pull": {"relationships.data_objects": {"$in": do_ids}}},
    )
    db.configurations.update_many(
        filter=filter,
        update={"$pull": {"relationships.data_objects": {"$in": do_ids}}},
    )
    db.data_objects.delete_many({"colabfit-id": {"$in": do_ids}})


# # Find DOs that point to multiple datasets and remove MP DS-id from relationships
# while True:
#     dos_mult = db.data_objects.find(
#         {"relationships.datasets": ds_id}, {"colabfit-id": 1}
#     ).limit(1)
#     do_ids = list(do["colabfit-id"] for do in dos_mult)
#     print(len(do_ids))
#     if len(do_ids) == 0:
#         break

#     db.property_instances.update_many(
#         filter=pi_filter,
#         update={"$pull": {"relationships.data_objects": {"$in": do_ids}}},
#     )
#     db.configurations.update_many(
#         filter=co_filter,
#         update={"$pull": {"relationships.data_objects": {"$in": do_ids}}},
#     )
#     db.data_objects.update_many(
#         {"colabfit-id": {"$in": do_ids}},
#         {"$pull": {"relationships.datasets": {"$in": do_ids}}},
#     )

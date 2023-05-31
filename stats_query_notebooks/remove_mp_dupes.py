from pymongo import MongoClient

client = MongoClient("mongodb://localhost:5000/")
db = client["colabfit-2023-5-16"]
ds_id = "DS_2wiwpg3oyupj_0"

dos = db.data_objects.find({"relationships.datasets": [ds_id]}, {"colabfit-id": 1})

# Find DOs that only point to MP dataset and delete them
for do in dos:
    do_id = do["colabfit-id"]
    print(do_id)
    db.property_instances.update_many(
        filter={"relationships.data_objects": do_id},
        update={"$pull": {"relationships.data_objects": do_id}},
    )
    db.configurations.update_many(
        filter={"relationships.data_objects": do_id},
        update={"$pull": {"relationships.data_objects": do_id}},
    )
    db.data_objects.delete_one({"colabfit-id": do_id})


# Find DOs that point to multiple datasets and remove MP DS-id from relationships
dos_mult = db.data_objects.find({"relationships.datasets": ds_id}, {"colabfit-id": 1})


for do in dos_mult:
    do_id = do["colabfit-id"]
    print(do_id)
    db.property_instances.update_many(
        filter={"relationships.data_objects": do_id},
        update={"$pull": {"relationships.data_objects": do_id}},
    )
    db.configurations.update_many(
        filter={"relationships.data_objects": do_id},
        update={"$pull": {"relationships.data_objects": do_id}},
    )
    db.data_objects.update_one(
        {"colabfit-id": do_id}, {"$pull": {"relationships.datasets": ds_id}}
    )

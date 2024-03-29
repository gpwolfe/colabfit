"""
Script to update the OC20 dataset by batches, iterating across files of DO-ids
generated by oc20_is2res_batch_configs.py.

usage:

python oc20_is2res_batch_ds_update.py -i localhost -r 27017 -d oc20 -p 4 \
    -o DS_hcrq1don4k1r_0_do_ids_2024_01_05

"""

from argparse import ArgumentParser
import asyncio
from collections import defaultdict
import datetime
from functools import partial, wraps
import logging
from pathlib import Path
import pymongo
from multiprocessing import Pool
import sys
import time
import numpy as np
from colabfit.tools.database import MongoDatabase
from colabfit import (
    SHORT_ID_STRING_NAME,
)
from tqdm import tqdm

START_BATCH = 0
MAX_AUTO_RECONNECT_ATTEMPTS = 100


AUTHORS = [
    "Lowik Chanussot",
    "Abhishek Das",
    "Siddharth Goyal",
    "Thibaut Lavril",
    "Muhammed Shuaibi",
    "Morgane Riviere",
    "Kevin Tran",
    "Javier Heras-Domingo",
    "Caleb Ho",
    "Weihua Hu",
    "Aini Palizhati",
    "Anuroop Sriram",
    "Brandon Wood",
    "Junwoong Yoon",
    "Devi Parikh",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
]

PUBLICATION = "https://doi.org/10.1021/acscatal.0c04525"
DATA_LINK = "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md"
OTHER_LINKS = ["https://arxiv.org/abs/2010.09990"]
LINKS = [
    "https://arxiv.org/abs/2010.09990",
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md",
]
DS_DESC = (
    "Configurations for the initial structure to relaxed energy "
    "(IS2RE) and initial structure to relaxed structure (IS2RS) tasks of "
    'Open Catalyst 2020 (OC20). Dataset corresponds to the "All IS2RE/S training '
    'data split under the "Relaxation Trajectories" '
    "section of the Open Catalyst Project GitHub page."
)
DATASET = "OC20_IS2RES_train"


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                if wait_t > 1800:
                    wait_t = 1800  # cap at 1/2 hour
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


# function dependency for aggregate configuration summary
@auto_reconnect
def agg(hashes, db, uri):
    from colabfit.tools.database import MongoDatabase

    client = MongoDatabase(db, uri=uri)
    proxy = {
        "nsites": 0,
        "chemical_systems": set(),
        "elements": [],
        "individual_elements_ratios": {},
        "total_elements_ratios": {},
        "chemical_formula_reduced": set(),
        "chemical_formula_anonymous": set(),
        "chemical_formula_hill": set(),
        "nperiodic_dimensions": set(),
        "dimension_types": set(),
    }

    docs = client.configurations.find(
        {"hash": {"$in": hashes}},
        {
            "nsites": 1,
            "elements": 1,
            "elements_ratios": 1,
            "total_elements_ratios": 1,
            "nperiodic_dimensions": 1,
            "dimension_types": 1,
            "elements_ratios": 1,
            "chemical_formula_anonymous": 1,
            "chemical_formula_hill": 1,
            "chemical_formula_reduced": 1,
        },
    )
    while True:
        for doc in docs:
            proxy["nsites"] += doc["nsites"]
            proxy["chemical_systems"].add("".join(doc["elements"]))

            for e, er in zip(doc["elements"], doc["elements_ratios"]):
                if e not in proxy["elements"]:
                    proxy["elements"].append(e)
                    proxy["total_elements_ratios"][e] = er * doc["nsites"]
                    proxy["individual_elements_ratios"][e] = {np.round(er, decimals=2)}
                else:
                    proxy["total_elements_ratios"][e] += er * doc["nsites"]
                    proxy["individual_elements_ratios"][e].add(np.round(er, decimals=2))

            proxy["chemical_formula_reduced"].add(doc["chemical_formula_reduced"])
            proxy["chemical_formula_anonymous"].add(doc["chemical_formula_anonymous"])
            proxy["chemical_formula_hill"].add(doc["chemical_formula_hill"])

            proxy["nperiodic_dimensions"].add(doc["nperiodic_dimensions"])
            proxy["dimension_types"].add(tuple(doc["dimension_types"]))
        return proxy


@auto_reconnect
def aggregate_configuration_summaries(client, hashes, verbose=False):
    """
    Uses agg to gather batch of config aggregated info
    """
    aggregated_info = {
        "nconfigurations": len(hashes),
        "nsites": 0,
        "nelements": 0,
        "chemical_systems": set(),
        "elements": [],
        "individual_elements_ratios": {},
        "total_elements_ratios": {},
        "chemical_formula_reduced": set(),
        "chemical_formula_anonymous": set(),
        "chemical_formula_hill": set(),
        "nperiodic_dimensions": set(),
        "dimension_types": set(),
    }

    n = len(hashes)
    k = client.nprocs
    chunked_hashes = [
        hashes[i * (n // k) + min(i, n % k) : (i + 1) * (n // k) + min(i + 1, n % k)]
        for i in range(k)
    ]
    s = time.time()
    with Pool(k) as pool:
        aggs = pool.map(
            partial(agg, db=client.database_name, uri=client.uri), chunked_hashes
        )
    for a in aggs:
        aggregated_info["nsites"] += a["nsites"]
        aggregated_info["chemical_systems"].update(a["chemical_systems"])

        for e, er in zip(a["elements"], a["individual_elements_ratios"]):
            if e not in aggregated_info["elements"]:
                aggregated_info["elements"].append(e)
                aggregated_info["total_elements_ratios"][e] = a[
                    "total_elements_ratios"
                ][e]
                aggregated_info["individual_elements_ratios"][e] = a[
                    "individual_elements_ratios"
                ][e]
            else:
                aggregated_info["total_elements_ratios"][e] += a[
                    "total_elements_ratios"
                ][e]
                aggregated_info["individual_elements_ratios"][e].update(
                    a["individual_elements_ratios"][e]
                )

        aggregated_info["chemical_formula_reduced"].update(
            a["chemical_formula_reduced"]
        )
        aggregated_info["chemical_formula_anonymous"].update(
            a["chemical_formula_anonymous"]
        )
        aggregated_info["chemical_formula_hill"].update(a["chemical_formula_hill"])

        aggregated_info["nperiodic_dimensions"].update(a["nperiodic_dimensions"])
        aggregated_info["dimension_types"].update(a["dimension_types"])

    for e in aggregated_info["elements"]:
        aggregated_info["nelements"] += 1
        aggregated_info["total_elements_ratios"][e] /= aggregated_info["nsites"]
        aggregated_info["individual_elements_ratios"][e] = list(
            aggregated_info["individual_elements_ratios"][e]
        )

    aggregated_info["chemical_systems"] = list(aggregated_info["chemical_systems"])
    aggregated_info["chemical_formula_reduced"] = list(
        aggregated_info["chemical_formula_reduced"]
    )
    aggregated_info["chemical_formula_anonymous"] = list(
        aggregated_info["chemical_formula_anonymous"]
    )
    aggregated_info["chemical_formula_hill"] = list(
        aggregated_info["chemical_formula_hill"]
    )
    aggregated_info["nperiodic_dimensions"] = list(
        aggregated_info["nperiodic_dimensions"]
    )
    aggregated_info["dimension_types"] = list(aggregated_info["dimension_types"])
    print("Configuration aggregation time:", time.time() - s)
    return aggregated_info


@auto_reconnect
def aggregate_data_object_info(client, do_hashes, verbose=False):
    """
    Query data objects by hash, getting associated configurations and
    returning the aggregated configuration data

    Uses aggregate_configuration_summaries to gather info from configurations
    pointing to current batch of DOs
    """
    if isinstance(do_hashes, str):
        do_hashes = [do_hashes]

    property_types = defaultdict(int)
    aggregated_info = {}
    co_ids = []

    for doc in tqdm(
        client.data_objects.find(
            {"hash": {"$in": do_hashes}},
            {"property_types": 1, "relationships": 1},
        )
    ):
        for p_type in doc["property_types"]:
            property_types[p_type] += 1
        # get co hashes
        co_ids.append(doc["relationships"][0]["configuration"].replace("CO_", ""))
    config_agg = aggregate_configuration_summaries(client, co_ids, verbose=verbose)

    aggregated_info.update(config_agg)
    aggregated_info["property_types"] = list(property_types.keys())
    aggregated_info["property_types_counts"] = list(property_types.values())
    aggregated_info["nconfigurations"] = len(co_ids)
    return aggregated_info


def update_ds_agg_info(aggregated_info, new_info):
    """
    Updates a dict of aggregated info

    aggregated_info = current ds agg info
    new_info = new info to add to current agg info
    """
    proxy = {
        "nconfigurations": aggregated_info["nconfigurations"],
        "nsites": aggregated_info["nsites"],
        "elements": aggregated_info["elements"],
        "total_elements_ratios": aggregated_info["total_elements_ratios"],
        "nperiodic_dimensions": set(aggregated_info["nperiodic_dimensions"]),
        "dimension_types": set([tuple(x) for x in aggregated_info["dimension_types"]]),
    }
    property_types = dict(
        zip(
            aggregated_info["property_types"],
            aggregated_info["property_types_counts"],
        )
    )
    new_num_e = defaultdict(int)
    proxy["nsites"] += new_info["nsites"]
    proxy["nconfigurations"] += new_info["nconfigurations"]

    for e in new_info["elements"]:
        new_num_e[e] += 1
        if e not in proxy["elements"]:
            proxy["elements"].append(e)

    proxy["nperiodic_dimensions"].update(new_info["nperiodic_dimensions"])
    proxy["dimension_types"].update(new_info["dimension_types"])
    for p_type, p_type_count in zip(
        new_info["property_types"], new_info["property_types_counts"]
    ):
        if p_type in property_types:
            property_types[p_type] += p_type_count
        else:
            property_types[p_type] = p_type_count

    # Reset elements and calculate new total elements ratios
    proxy["nelements"] = 0
    old_nsites = aggregated_info["nsites"]
    proxy["elements"] = sorted(proxy["elements"])
    for e in proxy["elements"]:
        old_ratio = aggregated_info["total_elements_ratios"].get(e, 0)
        old_num_e = old_ratio * old_nsites
        proxy["nelements"] += 1
        proxy["total_elements_ratios"][e] = (old_num_e + new_num_e[e]) / proxy["nsites"]

    proxy["nperiodic_dimensions"] = list(proxy["nperiodic_dimensions"])
    proxy["dimension_types"] = list(aggregated_info["dimension_types"])
    property_types = dict(sorted(property_types.items()))
    proxy["property_types"] = list(property_types.keys())
    proxy["property_types_counts"] = list(property_types.values())
    return proxy


@auto_reconnect
async def insert_dos_to_dataset(args, ds_id=None, verbose=False, fp=None):
    """
    Uses aggregate_data_object_info to gather info from a new batch of DOs

    Uses update_ds_agg_info to update an existing dataset's aggregated info
    and push to the new data to the database. Does not change dataset version.
    """
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    print(client.name, "\n\n")
    with open(fp, "r") as f:
        do_hashes = [id.strip() for id in f.readlines()]
    if isinstance(do_hashes, str):
        do_hashes = [do_hashes]

    # Remove possible duplicates
    do_hashes = list(set(do_hashes))

    new_aggregated_info = {}

    for k, v in aggregate_data_object_info(client, do_hashes, verbose=verbose).items():
        new_aggregated_info[k] = v

    # Insert necessary aggregated info into its collection
    client.insert_aggregated_info(new_aggregated_info, "dataset", ds_id)

    current_ds_agg_info = client.datasets.find(
        {SHORT_ID_STRING_NAME: ds_id},
        {
            f"aggregated_info.{key}": 1
            for key in [
                "nconfigurations",
                "nsites",
                "nelements",
                "elements",
                "total_elements_ratios",
                "nperiodic_dimensions",
                "dimension_types",
                "property_types",
                "property_types_counts",
            ]
        },
    )
    current_ds_agg_info = next(current_ds_agg_info)
    current_ds_agg_info = current_ds_agg_info["aggregated_info"]

    # Now update the dataset with the latest iteration of aggregated info
    for item in [
        "individual_elements_ratios",
        "chemical_systems",
        "chemical_formula_anonymous",
        "chemical_formula_hill",
        "chemical_formula_reduced",
    ]:
        new_aggregated_info.pop(item)
    updated_aggregated_info = update_ds_agg_info(
        current_ds_agg_info, new_aggregated_info
    )

    client.datasets.update_one(
        {"colabfit-id": ds_id},
        {
            "$set": {
                "aggregated_info": updated_aggregated_info,
                "last_modified": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        },
        upsert=True,
        hint="hash",
    )
    with open("ds_batches.txt", "a") as f:
        f.write(f"{fp}\n")
    client.close()


@auto_reconnect
async def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    parser.add_argument("-r", "--port", type=int, help="Target port for MongoDB")
    parser.add_argument(
        "-o", "--do", type=str, help="Directory of data_object id files"
    )
    parser.add_argument(
        "--ds", type=str, help="Dataset id to update with new data objects"
    )
    args = parser.parse_args(argv)
    do_ids_dir = Path(args.do)
    ds_id = args.ds
    nprocs = args.nprocs
    client = MongoDatabase(
        args.db_name, nprocs=nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    # ds_id = "_".join(do_ids_dir.parts[-1].split("_")[:3])
    fps = sorted(list(do_ids_dir.rglob("*.txt")))

    # if START_BATCH == 0:
    # check if dataset exists
    if client.datasets.find_one({"colabfit-id": ds_id}) is not None:
        tasks = [
            asyncio.create_task(insert_dos_to_dataset(args, ds_id=ds_id, fp=fp))
            for fp in fps
        ]
        await asyncio.gather(*tasks)

    else:
        # Create the dataset with the first file
        with open(fps[0], "r") as f:
            do_ids0 = [id.strip() for id in f.readlines()]
        client.insert_dataset(
            do_hashes=do_ids0,
            ds_id=ds_id,
            name=DATASET,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
            description=DS_DESC,
            verbose=False,
            data_license="https://creativecommons.org/licenses/by/4.0/",
        )
        client.close()
        # Create async tasks for update dataset
        tasks = [
            asyncio.create_task(insert_dos_to_dataset(args, ds_id=ds_id, fp=fp))
            for fp in fps[1:]
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    args = sys.argv[1:]
    asyncio.run(main(args))

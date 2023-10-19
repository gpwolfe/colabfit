"""
author: Gregory Wolfe

Functions for generating schema.org JSON elements for dataset pages to be
searchable by the Google Dataset Search engine.

generate_schema_all will generate JSON elements for all datasets in a database

generate_schema_single will generate JSON elements for a single given dataset
"""
from argparse import ArgumentParser
import json
import pymongo
import sys


def generate_schema_all(db: pymongo.database.Database):
    collection = db["datasets"]
    for dataset in collection.find(
        {}, {"authors": 1, "links": 1, "name": 1, "description": 1, "license": 1}
    ):
        authors = dataset.get("authors", [])
        links = dataset.get("links", [])
        name = dataset.get("name", "")
        license = dataset.get("license", "")
        description = dataset.get("description", "")
        # Create a JSON object conforming to schema.org
        schema_org_data = {
            "@context": "http://schema.org",
            "@id": name,
            "@type": "Dataset",
            "name": name,
            "creator": [{"@type": "Person", "name": author} for author in authors],
            "url": [{"@type": "URL", "url": link} for link in links],
            "description": description,
            "license": license,
        }

        # Serialize the JSON object to a string
        json_document = json.dumps(schema_org_data, indent=2)

        yield json_document


def generate_schema_single(db: pymongo.database.Database, ds: str):
    """
    Return JSON schema for a single dataset

    db: pymongo database
    ds: name of dataset
    """
    collection = db["datasets"]
    dataset = collection.find_one(
        {"name": ds},
        {"authors": 1, "links": 1, "name": 1, "description": 1, "license": 1},
    )
    authors = dataset.get("authors", [])
    links = dataset.get("links", [])
    name = dataset.get("name", "")
    license = dataset.get("license", "")
    description = dataset.get("description", "")
    # Create a JSON object conforming to schema.org
    schema_org_data = {
        "@context": "http://schema.org",
        "@id": name,
        "@type": "Dataset",
        "name": name,
        "creator": [{"@type": "Person", "name": author} for author in authors],
        "url": [{"@type": "URL", "url": link} for link in links],
        "description": description,
        "license": license,
    }

    # Serialize the JSON object to a string
    json_document = json.dumps(schema_org_data, indent=2)

    return json_document


if __name__ == "__main__":
    args = sys.argv[1:]
    parser = ArgumentParser()
    parser.add_argument("-i", help="IP address for MongoDB client")
    parser.add_argument(
        "-p", type=int, default=27017, help="Port to use for MongoDB client"
    )
    parser.add_argument(
        "-b", type=str, help="Name of database on MongoDB client to use"
    )
    parser.add_argument(
        "-s",
        default=None,
        help="Dataset name: optional. If not defined, generates schema for all datasets",
    )
    args = parser.parse_args(args)
    client = pymongo.MongoClient(host=f"mongodb://{args.i}:{args.p}")
    db = client[args.b]
    ds = args.s
    if ds is None:
        print(list(generate_schema_all(db)))
    else:
        print(generate_schema_single(db, ds))

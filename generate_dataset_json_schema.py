"""
author: Gregory Wolfe

Functions for generating schema.org JSON elements for dataset pages to be
searchable by the Google Dataset Search engine.

generate_schema_all will generate JSON elements for all datasets in a database

generate_schema_single will generate JSON elements for a single given dataset
"""

import json
import pymongo


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

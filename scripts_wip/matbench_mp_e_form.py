"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from ase.atoms import Atoms

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("").cwd()
DATASET_NAME = ""
LICENSE = ""

PUBLICATION = ""
DATA_LINK = ""
# OTHER_LINKS = []

AUTHORS = ["Alexander Dunn", "Qi Wang", "Alex Ganose", "Daniel Dopp", "Anubhav Jain"]
DATASET_DESC = ""
ELEMENTS = [""]
GLOB_STR = "*.*"

PI_METADATA = {
    "software": {"value": ""},
    "method": {"value": ""},
    # "basis-set": {"field": "basis_set"}
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "GPa"},
    #         "volume-normalized": {"value": True, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

CO_METADATA = {
    "enthalpy": {"field": "h", "units": "Ha"},
    "zpve": {"field": "zpve", "units": "Ha"},
}

CSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]


def yield_structure(fp):
    with open(fp, "r") as f:
        line = f.readline()
        start_index = 0
        in_object = False
        while True:
            start_marker = line.find("[{", start_index)
            if start_marker == -1:
                break  # No more objects found

            if '@module": "pymatgen.core.structure",' in line[start_marker:]:
                in_object = True
                obj_start = start_marker

            if in_object:
                end_marker = line.find('"@version":', start_marker)
                if end_marker != -1:
                    version_end = line.find(
                        ",", end_marker + 11
                    )  # Skip "@version": and its value
                    end_index = line.find(
                        "]", version_end
                    )  # Find ending square bracket and curly brace
                    object_str = line[obj_start : end_index + 1]
                    # yield(object_str)
                    yield json.loads(object_str)
                    in_object = False
                    start_index = end_index + 2
                else:
                    start_index = (
                        start_marker + 1
                    )  # Continue searching within the object
            else:
                start_index = start_marker + 1


def reader(fp):
    for row in yield_structure(fp):
        atom = row[0]
        eform = row[1]
        pos = [site["xyz"] for site in atom["sites"]]
        sym = [site["species"][0]["element"] for site in atom["sites"]]
        cell = atom["lattice"]["matrix"]

        config = Atoms(positions=pos, symbols=sym, cell=cell)
        config.info["eform"] = eform
        yield config


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_ids = []
    for i, (name, query, desc) in enumerate(CSS):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=query,
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

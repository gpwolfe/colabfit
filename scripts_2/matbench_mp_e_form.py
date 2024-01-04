"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

Matbench files are formatted as a single massive JSON object on one line
I have reformatted so that one configuration appears per line in a new
json file. The function used for reformatting appears at the bottom of this file.
Reformatting allows the dataset to ingest in < 1min as opposed to ~1hour+
The index object that precedes the configurations+formation energies
in the original file is omitted, as this is a simple cardinal index. This is
recreated in the reader function.

"""
from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from ase.atoms import Atoms
from tqdm import tqdm

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase


DATASET_FP = Path("data/matbench")
DATASET_NAME = "Matbench_mp_e_form"
LICENSE = "https://opensource.org/licenses/MIT"

PUBLICATION = "https://doi.org/10.1038/s41524-020-00406-3"
DATA_LINK = "https://matbench.materialsproject.org/"
OTHER_LINKS = ["https://doi.org/10.1016/j.commatsci.2014.10.037"]

AUTHORS = ["Alexander Dunn", "Qi Wang", "Alex Ganose", "Daniel Dopp", "Anubhav Jain"]
DATASET_DESC = (
    "Matbench v0.1 test dataset for predicting DFT formation energy from "
    "structure. Adapted from Materials Project database. Entries having "
    "formation energy more than 2.5eV and those containing noble gases are removed. "
    "Retrieved April 2, 2019. For benchmarking w/ nested cross validation, the order "
    "of the dataset must be identical to the retrieved data; refer to the "
    "Automatminer/Matbench publication for more details."
    "Matbench is an automated leaderboard for benchmarking state of the art "
    "ML algorithms predicting a diverse range of solid materials' properties. "
    "It is hosted and maintained by the Materials Project."
)
ELEMENTS = None
GLOB_STR = "matbench_mp_e_form_structures.json"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT"},
    # "basis-set": {"field": "basis_set"}
}

PROPERTY_MAP = {
    "formation-energy": [
        {
            "energy": {"field": "eform", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "volume": {"field": "volume"},
    "materials-project-version": {"field": "materials-project-version"},
}

CSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]


def reader(fp):
    with open(fp, "r") as f:
        for i, line in tqdm(enumerate(f)):
            row = json.loads(line)
            atom = row[0]
            eform = row[1]
            pos = [site["xyz"] for site in atom["sites"]]
            sym = [site["species"][0]["element"] for site in atom["sites"]]
            cell = atom["lattice"]["matrix"]

            config = Atoms(positions=pos, symbols=sym, cell=cell)
            config.info["eform"] = eform
            config.info["name"] = f"matbench_mp_e_form_{i}"
            config.info["volume"] = atom["lattice"]["volume"]
            config.info["materials-project-version"] = atom["@version"]
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
    with open("formation_energy.json", "r") as f:
        formation_energy_pd = json.load(f)
    client.insert_property_definition(formation_energy_pd)

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

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])


# def structures_to_file(fp, outfile):
#     with open(fp, "r") as f:
#         with open(outfile, "w") as fout:
#             line = f.readline()
#             start_index = 0
#             in_object = False
#             counter = 0
#             while True:
#                 start_marker = line.find("[{", start_index)
#                 if start_marker == -1:
#                     break  # No more objects found

#                 if '@module": "pymatgen.core.structure",' in line[start_marker:]:
#                     in_object = True
#                     obj_start = start_marker

#                 if in_object:
#                     end_marker = line.find('"@version":', start_marker)
#                     if end_marker != -1:
#                         version_end = line.find(
#                             ",", end_marker + 11
#                         )  # Skip "@version": and its value
#                         end_index = line.find(
#                             "]", version_end
#                         )  # Find ending square bracket and curly brace
#                         object_str = line[obj_start : end_index + 1]
#                         # json.dumps()
#                         fout.write(f"{object_str}\n")
#                         if counter % 100 == 0:
#                             print(counter)
#                         counter += 1

#                         in_object = False
#                         start_index = end_index + 2
#                     else:
#                         start_index = (
#                             start_marker + 1
#                         )  # Continue searching within the object
#                 else:
#                     start_index = start_marker + 1

"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:

For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

Keys
['atoms',
'desc',
'form_energy',
'id',
'stable',
'total_energy']
"""


from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase


from colabfit.tools.property_definitions import potential_energy_pd


DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "CFID_OQMD_460k.json"
DS_NAME = "JARVIS_CFID_OQMD"
DS_DESC = (
    "The JARVIS_CFID_OQMD dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the Open Quantum Materials Database (OQMD), created to hold "
    "information about the electronic structure and stability of organic materials "
    "for the purpose of aiding in materials discovery. Calculations were performed "
    "at the DFT level of theory, using the PAW-PBE functional implemented by VASP. "
    "This dataset also includes classical force-field inspired descriptors (CFID) for "
    "each configuration. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

PUBLICATION = "https://doi.org/10.1038/npjcompumats.2015.10"
DATA_LINK = "https://ndownloader.figshare.com/files/24981170"
OTHER_LINKS = ["https://jarvis.nist.gov/"]

LINKS = [
    "https://doi.org/10.1038/npjcompumats.2015.10",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/24981170",
]
AUTHORS = [
    "Scott Kirklin",
    "James E Saal",
    "Bryce Meredig",
    "Alex Thompson",
    "Jeff W Doak",
    "Muratahan Aykol",
    "Stephan Rühl",
    "Chris Wolverton",
]
ELEMENTS = None


PROPERTY_MAP = {
    "formation-energy": [
        # {
        #     "energy": {"field": "total_energy", "units": "eV"},
        #     "per-atom": {"value": False, "units": None},
        #     "_metadata": {
        #         "software": {"value": "VASP"},
        #         "method": {"value": "DFT-PBE"},
        #     },
        # },
        {
            "energy": {"field": "form_energy", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "input": {
                    "value": {
                        "ediff": {"value": 10e-4, "units": "eV/atom"},
                        "kpoint-scheme": "gamma-centered",
                        "encut": {"value": 520, "units": "eV"},
                        "ismear": {"relaxation": -1, "static": -5},
                        "sigma": 0.2,
                        "ibrion": 1,
                        "ediffg": {"value": 10e-3, "units": "eV/atom"},
                    }
                },
            },
        },
    ],
}


with open("formation_energy.json", "r") as f:
    formation_energy_pd = json.load(f)
# with open("band_gap.json", "r") as f:
#     band_gap_pd = json.load(f)


def get_client(argv):
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
    return client


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
        data = data
    configs = []
    for i, row in enumerate(data):
        atoms = row.pop("atoms")
        if atoms["cartesian"] is True:
            config = AtomicConfiguration(
                positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        else:
            config = AtomicConfiguration(
                scaled_positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )

        config.info["name"] = f"{fp.stem}_{i}"

        for key, val in row.items():
            if isinstance(val, str) and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, dict) and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (isinstance(val, float) or isinstance(val, int)) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
            else:
                pass
        configs.append(config)
    return configs


def main(argv):
    client = get_client(argv)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB,
        generator=False,
    )

    # client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(formation_energy_pd)
    client.insert_property_definition(potential_energy_pd)

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
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


CO_KEYS = [
    # "atoms",
    "desc",
    # "form_energy",
    "id",
    "stable",
    "total_energy",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

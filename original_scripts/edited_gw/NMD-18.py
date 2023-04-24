#!/usr/bin/env python
# coding: utf-8
"""
File notes
-------------
Check whether custom properties imported here should be used as properties or added to
metadata (possibly uncomment definitions in this file to avoid imports)
"""
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data

DATASET = "NMD-18"
DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/NMD_18")

AUTHORS = [
    "Christopher Sutton",
    "Luca M. Ghiringhelli",
    "Takenori Yamamoto",
    "Yury Lysogorskiy",
    "Lars Blumenthal",
    "Thomas Hammerschmidt",
    "Jacek R. Golebiowski",
    "Xiangyue Liu",
    "Angelo Ziletti",
    "Matthias Scheffler",
]
LINKS = [
    "https://doi.org/10.1038/s41524-019-0239-3",
    "https://qmml.org/datasets.html",
]
DS_DESC = (
    "3,000 Al-Ga-In sesquioxides with energies and band gaps. Relaxed and Vegard's "
    "Law geometries with formation energy and band gaps at DFT-PBE level of theory of "
    "(Alx-Gay-Inz)2O3 oxides, x+y+z=1. Contains all structures from the NOMAD 2018 "
    "Kaggle challenge training and leaderboard data. The formation energy and "
    "bandgap energy were computed by using the PBE exchange-correlation DFT "
    "functional with the all-electron electronic structure code FHI-aims with "
    "tight setting."
)


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    configurations = load_data(
        file_path=DATASET_FP / "NMD-18r-reformatted.xyz",
        file_format="xyz",
        name_field="sg",
        elements=["Al", "Ga", "In", "O"],
        default_name="NMD-18r",
        verbose=True,
        generator=False,
    )
    """
    configurations += load_data(
        file_path=DATASET_FP / "NMD-18u-reformatted.xyz",
        file_format='xyz',
        name_field='sg',
        elements=['Al', 'Ga', 'In','O'],
        default_name='NMD-18u',
        verbose=True,
        generator=False
    )
    """
    cs_list = set()
    for c in configurations:
        cs_list.add(*c.info["_name"])
    """
    formation_property_definition = {
        "property-id": "formation-energy",
        "property-name": "formation-energy",
        "property-title": "formation energy",
        "property-description": "formation energy PER ATOM",
        "energy": {
            "type": "float",
            "has-unit": True,
            "extent": [],
            "required": True,
            "description": "formation energy PER ATOM",
        },
    }

    band_property_definition = {
        "property-id": "band-gap",
        "property-name": "band-gap",
        "property-title": "band gap",
        "property-description": "bandgap PER UNIT CELL",
        "energy": {
            "type": "float",
            "has-unit": True,
            "extent": [],
            "required": True,
            "description": "bandgap PER UNIT CELL",
        },
    }
    """
    # client.insert_property_definition('/home/ubuntu/calc_notebook/formation-energy.json')
    # client.insert_property_definition('/home/ubuntu/calc_notebook/band-gap.json')

    property_map = {
        "formation-energy": [
            {
                "energy": {"field": "fe", "units": "eV"},
                "_metadata": {
                    "software": {"value": "FHI-aims"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "band-gap": [
            {
                "energy": {"field": "bg", "units": "eV"},
                "_metadata": {
                    "software": {"value": "FHI-aims"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }

    def tform(c):
        c.info["per-atom"] = False

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

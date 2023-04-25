#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/W-14/w-14.xyz"
)
DATASET = "W-14"


LINKS = [
    "https://doi.org/10.1103/PhysRevB.90.104108",
    "https://qmml.org/datasets.html",
]
AUTHORS = ["Wojciech J. Szlachta", "Albert P. Bartók", "Gábor Csányi"]
DS_DESC = (
    "158,000 diverse atomic environments of elemental tungsten."
    "Includes DFT-PBE energies, forces and stresses for tungsten; periodic "
    "unit cells in the range of 1-135 atoms, including bcc primitive cell, "
    "128-atom bcc cell, vacancies, low index surfaces, gamma-surfaces, and "
    "dislocation cores."
)


def tform(c):
    c.info["per-atom"] = False


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
        file_path=DATASET_FP,
        file_format="xyz",
        name_field="config_type",
        elements=["W"],
        default_name=DATASET,
        verbose=True,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "CASTEP"},
                    "method": {"value": "DFT-PBE"},
                    "ecut": {"value": "600 eV"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "CASTEP"},
                    "method": {"value": "DFT-PBE"},
                    "ecut": {"value": "600 eV"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "software": {"value": "CASTEP"},
                    "method": {"value": "DFT-PBE"},
                    "ecut": {"value": "600 eV"},
                },
            }
        ],
    }

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

    cs_regexes = {
        "slice_sample": "Configurations of slice sample",
        "md_bulk": "Configurations of bulk state",
        "surface": "Configurations of surface",
        "vacancy": "Configurations of vacancy",
        "gamma_surface": "Configurations of gamma_surface",
        "dislocation_quadrupole": "Configurations of dislocation quadrupole",
    }

    # will check what these states mean later
    cs_names = [
        "slice sample",
        "bulk state",
        "surface",
        "vacancy",
        "gamma surface",
        "dislocation quadrupole",
    ]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=cs_names[i],
            description=desc,
            query={"names": {"$regex": regex}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
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

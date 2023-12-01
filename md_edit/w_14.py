#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
    cauchy_stress_pd,
)

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/W-14/w-14.xyz"
)
DATASET_FP = Path().cwd().parent / "data/w_14/w-14.xyz"
DATASET = "W-14"

PUBLICATION = "https://doi.org/10.1103/PhysRevB.90.104108"
DATA_LINK = "https://qmml.org/datasets.html"
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
PI_MD = {
    "software": {"value": "CASTEP 6.01"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "encut": {"value": 600, "units": "eV"},
            "pseudopotential": "Ultrasoft (valence 5[s^2]5[p^6]5[d^4]6[s^2])",
            "kspacing": {"value": 0.015, "units": "Ang^-1"},
            "smearing": "Gaussian",
            "smearing-width": {"value": 0.1, "units": "eV"},
        }
    },
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "force", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "virial", "units": "eV"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="xyz",
        name_field="config_type",
        elements=["W"],
        default_name=DATASET,
        verbose=True,
        generator=False,
    )

    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
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
            ds_id=ds_id,
            description=desc,
            query={"names": {"$regex": regex}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

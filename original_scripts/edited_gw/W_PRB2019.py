#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

import numpy as np

from colabfit.tools.database import MongoDatabase, load_data

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/"
    "acclab_helsinki/W/2019-05-24/training-data"
)
DATASET = "W_PRB2019"
LINKS = [
    "https://doi.org/10.1103/PhysRevB.100.144105",
    "https://gitlab.com/acclab/gap-data/-/tree/master/W/2019-05-24",
]
AUTHORS = [
    "J. Byggmästar",
    "A. Hamedani",
    "K. Nordlund",
    "F. Djurabekova",
]
DS_DESC = (
    "This dataset was originally designed to fit a GAP "
    "potential with a specific focus on properties relevant for simulations "
    "of radiation-induced collision cascades and the damage they produce, "
    "including a realistic repulsive potential for short-range many-body "
    "cascade dynamics and a good description of the liquid phase."
)


def tform(c):
    c.info["per-atom"] = False

    if "virial" in c.info:
        c.info["virial"] = (
            c.info["virial"] / np.abs(np.linalg.det(np.array(c.cell)))
        ) * -160.21766208


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

    configurations = list(
        load_data(
            file_path=DATASET_FP / "db_W.xyz",
            file_format="xyz",
            name_field="config_type",
            elements=["W"],
            default_name=DATASET,
            verbose=True,
        )
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "GPa"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            co_md_map={"configuration_type": {"field": "config_type"}},
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    len(set(all_co_ids))

    len(set(all_pr_ids))

    cs_regexes = {
        "^sc": "Simple cubic crystals with random lattice distortions",
        "^liquid": "Liquid W with densities around the experimental density of "
        "17.6 g/cm^3",
        "short_range": "BCC crystals with random interstitial atom defects to capture "
        "short-range many-body dynamics",
        "^vacancy": "Single-vacancy configurations",
        "di-vacancy": "Divacancy configurations",
        "phonon": "MD snapshots taken at 1000K for three different volumes",
        "slice_sample": "Randomly distorted primitive bcc unit cells drawn from "
        "Szlachta et al.'s database",
        "fcc": "FCC crystals with random lattice distortions",
        "bcc_distorted": "BCC configurations with random strains up to +/- 30% to "
        "help train the far-from-equilibrium elastic response",
        "dimer": "Dimers to fit to the full dissociation curve starting from 1.1 "
        "angstrom",
        "surface_111": "(111) surface configurations",
        "C15": "C15 configurations with random lattice distortions",
        "dia": "Diamond configurations with random lattice distortions",
        "hcp": "HCP configurations with random lattice distortions",
        "surf_liquid": "Damaged and half-molten (110) and (100) surfaces",
        "surface_100": "(100) surface configurations",
        "^sia": "Configurations with single self-interstitial defects",
        "surface_112": "(112) surface configurations",
        "surface_110": "(110) surface configurations",
        "tri-vacancy": "Trivacancy configurations",
        "A15": "A15 configurations with random lattice distortions",
        "isolated_atom": "Isolated W atom",
        "di-SIA": "Configurations with two self-interstitial defects",
    }

    cs_names = [
        "SC",
        "liquid",
        "short_range",
        "vacancy",
        "divacancy",
        "phonon",
        "slice_sample",
        "FCC",
        "BCC_distorted",
        "dimer",
        "surface_111",
        "C15",
        "diamond",
        "HCP",
        "surface_liquid",
        "surface_100",
        "self-interstitial_defect",
        "surface_112",
        "surface_110",
        "trivacancy",
        "A15",
        "isolated_atom",
        "di-self-interstitial_defect",
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


"""
    configuration_label_regexes = {
        "phonon": "aimd",
        "hcp": "hcp",
        "^sc": ["sc", "strain"],
        "^liquid": "liquid",
        "short_range": ["bcc", "interstitial", "warning", "large_forces", "repulsive"],
        "^vacancy": "vacancy",
        "di-vacancy": ["vacancy", "divacancy"],
        "fcc": ["fcc", "strain"],
        "bcc_distorted": ["bcc", "strain"],
        "dimer": ["dimer", "warning", "large_forces", "repulsive"],
        "surface": "surface",
        "C15": ["c15", "strain"],
        "dia": ["diamond", "strain"],
        "surf_liquid": "surface",
        "^sia$": "interstitial",
        "tri-vacancy": ["vacancy", "divacancy", "trivacancy"],
        "A15": ["a15", "strain"],
        "isolated_atom": "isolated_atom",
        "di-SIA": "interstitial",
        "slice_sample": ["bcc", "strain"],
    }

    for regex, labels in configuration_label_regexes.items():
        client.apply_labels(
            dataset_id=ds_id,
            collection_name='configurations',
            query={'hash': {'$in': all_co_ids}, 'names': {'$regex': regex}},
            labels=labels,
            verbose=True
        )
    """


if __name__ == "__main__":
    main(sys.argv[1:])

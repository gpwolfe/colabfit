#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

import numpy as np

from colabfit.tools.database import MongoDatabase, load_data

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/"
    "acclab_helsinki/Ta/training-data/db_Ta.xyz"
)
DATASET = "Ta_PRM2019"


LINKS = [
    "https://doi.org/10.1103/PhysRevMaterials.4.093802",
    "https://gitlab.com/acclab/gap-data/-/tree/master",
]
AUTHORS = [
    "J. Byggmästar",
    "K. Nordlund",
    "F. Djurabekova",
]
DS_DESC = (
    "This dataset was designed to enable machine-learning "
    "of Ta elastic, thermal, and defect properties, as well as surface "
    "energetics, melting, and the structure of the liquid phase. The dataset "
    "was constructed by starting with the dataset from J. Byggmästar et al., "
    "Phys. Rev. B 100, 144105 (2019), then rescaling all of the "
    "configurations to the correct lattice spacing and adding in gamma "
    "surface configurations."
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
            file_path=DATASET_FP,
            file_format="xyz",
            name_field="config_type",
            elements=["Ta"],
            default_name=DATASET,
            verbose=True,
        )
    )

    # TODO
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

    cs_regexes = {
        "^liquid": "Liquid configurations with densities around the experimental "
        "density",
        "^sia": "Configurations with single self-interstitial defects",
        "^vacancy": "Single-vacancy configurations",
        "A15": "A15 configurations with random lattice distortions",
        "bcc_distorted": "BCC configurations with random strains up to +/- 30% to help "
        "train the far-from-equilibrium elastic response",
        "C15": "C15 configurations with random lattice distortions",
        "di-sia": "Configurations with two self-interstitial defects",
        "di-vacancy": "Divacancy configurations",
        "dia": "Diamond configurations with random lattice distortions",
        "dimer": "Dimers to fit to the full dissociation curve starting from 1.1 "
        "angstrom",
        "fcc": "FCC crystals with random lattice distortions",
        "gamma_surface": "Configurations representing the full gamma surface",
        "hcp": "HCP configurations with random lattice distortions",
        "isolated_atom": "Isolated W atom",
        "phonon": "MD snapshots taken at 1000K for three different volumes",
        "sc": "Simple cubic crystals with random lattice distortions",
        "short_range": "BCC crystals with random interstitial atom defects to capture "
        "short-range many-body dynamics",
        "slice_sample": "Randomly distorted primitive bcc unit cells drawn from "
        "Szlachta et al.'s database",
        "surf_liquid": "Damaged and half-molten (110) and (100) surfaces",
        "surface_100": "Configurations with single self-interstitial defects",
        "surface_110": "(110) surface configurations",
        "surface_111": "(111) surface configurations",
        "surface_112": "(112) surface configurations",
        "tri-vacancy": "Trivacancy configurations",
    }

    cs_names = [
        "liquid",
        "self_interstitial_defect",
        "vacancy",
        "A15",
        "bcc_distorted",
        "C15",
        "double_self_interstitial_defect",
        "divacancy",
        "diamond",
        "dimer",
        "FCC",
        "gamma_surface",
        "HCP",
        "isolated_atom",
        "phonon",
        "SC",
        "short_range",
        "slice_sample",
        "surface_liquid",
        "surface_100",
        "surface_110",
        "surface_111",
        "surface_112",
        "trivacancy",
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
        "A15": ["a15", "strain"],
        "bcc_distorted": ["bcc", "strain"],
        "C15": ["c15", "strain"],
        "di-sia": "interstitial",
        "di-vacancy": ["vacancy", "divacancy"],
        "dia": ["diamond", "strain"],
        "dimer": ["dimer", "warning", "large_forces", "repulsive"],
        "fcc": ["fcc", "strain"],
        "gamma_surface": "gamma_surface",
        "hcp": "hcp",
        "isolated_atom": "isolated_atom",
        "liquid": "liquid",
        "phonon": "aimd",
        "sc": ["sc", "strain"],
        "short_range": ["bcc", "interstitial", "warning", "large_forces", "repulsive"],
        "sia": "interstitial",
        "slice_sample": ["bcc", "strain"],
        "surf_liquid": "surface",
        "surface": "surface",
        "tri-vacancy": ["vacancy", "divacancy", "trivacancy"],
        "vacancy": "vacancy",
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

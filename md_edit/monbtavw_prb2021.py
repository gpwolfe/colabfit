#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

import numpy as np

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/"
    "acclab_helsinki/tabGAP_2b+3b_MoNbTaVW/db_HEA_reduced.xyz"
)
DATASET_FP = (
    Path().cwd().parent
    / "data/monbtavw_prb_2021/tabGAP_2b+3b_MoNbTaVW/db_HEA_reduced.xyz"
)
DATASET = "MoNbTaVW_PRB2021"


PUBLICATION = "https://doi.org/10.1103/PhysRevB.104.104101"
DATA_LINK = "https://doi.org/10.23729/1b845398-5291-4447-b417-1345acdd2eae"
LINKS = [
    "https://doi.org/10.1103/PhysRevB.104.104101",
    "https://doi.org/10.23729/1b845398-5291-4447-b417-1345acdd2eae",
]
AUTHORS = [
    "Jesper Byggmästar",
    "Kai Nordlund",
    "Flyura Djurabekova",
]
DS_DESC = (
    "This dataset was originally designed to fit a GAP "
    "model for the Mo-Nb-Ta-V-W quinary system that was used to study "
    "segregation and defects in the body-centered-cubic refractory "
    "high-entropy alloy MoNbTaVW."
)
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {"encut": {"value": 500, "units": "eV"}},
        "kspacing": {"value": 0.15, "units": "Ang^-1"},
        "sigma": "0.1",
    },
}


def tform(c):
    c.info["per-atom"] = False

    if "virial" in c.info:
        c.info["virial"] = (
            c.info["virial"] / np.abs(np.linalg.det(np.array(c.cell)))
        ) * -160.21766208


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
            "stress": {"field": "virial", "units": "GPa"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


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
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)
    configurations = list(
        load_data(
            file_path=DATASET_FP,
            file_format="xyz",
            name_field="config_type",
            elements=["Mo", "Nb", "Ta", "V", "W"],
            default_name=DATASET,
            verbose=True,
        )
    )

    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            ds_id=ds_id,
            co_md_map={"configuration_type": {"field": "config_type"}},
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    cs_regexes = {
        "^composition": "Ternary, quaternary, and quinary BCC alloys. 3 linearly "
        "spaced compositions were sampled, each with 3 different lattice constants. "
        "Atoms are randomly ordered and shifted slightly from their lattice "
        "positions.",
        "^liquid": "Liquid configurations",
        "^sia": "Configurations with single self-interstitial defects",
        "^vacancy": "Single-vacancy configurations",
        "bcc_distorted": "BCC configurations with random strains up to +/- 30% to help "
        "train the far-from-equilibrium elastic response",
        "binary_alloys": "Binary BCC alloys sampling 10 different concentrations from "
        "A=0.05 B=0.95 to A=0.95 B=0.05 and 3 different lattice constants for "
        "every composition. Atoms are randomly ordered and shifted slightly "
        "from their lattice positions.",
        "di-sia": "Configurations with two self-interstitial defects",
        "di-vacancy": "Divacancy configurations",
        "dimer": "Dimers to fit to the full dissociation curve starting from 1.1 "
        "angstrom",
        "gamma_surface": "Configurations representing the full gamma surface",
        "hea_ints": "1-5 interstitial atoms randomly inserted into HEA lattices and "
        "relaxed with a partially-trained tabGAP model",
        "hea_short_range": "Randomly placed unrelaxed interstitial atom in HEAs to fit "
        "repulsion inside crystals, making sure that the closest interatomic distance "
        "is not too short for DFT to be unreliable (> 1.35 Ang)",
        "hea_small": "Bulk equiatomic quinary HEAs. Atoms are randomly ordered and "
        "shifted slightly from their lattice positions. The lattice constant is "
        "randomised in the range 3-3.4 Angstrom",
        "hea_surface": "Disordered HEA surfaces, including some of the damaged/molten "
        "surface configurations from an existing pure W dataset that were "
        "turned into HEAs",
        "hea_vacancies": "1-5 vacancies randomly inserted into HEA lattices, then "
        "relaxed with a partially-trained tabGAP model",
        "isolated_atom": "Isolated W atom",
        "liquid_composition": "Liquid equiatomic binary, ternary, quaternary, and "
        "quinary alloys at different densities",
        "liquid_hea": "Liquid HEA configurations",
        "mcmd": "Equiatomic quinary alloys generated via active learning by running "
        "MCMD with a partially-trained tabGAP model.",
        "ordered_alloys": "Ordered binary, ternary, and quaternary alloys (always as a "
        "BCC lattice, but with different crystal symmetries of the elemental "
        "sublattices)",
        "phonon": "MD snapshots taken at 1000K for three different volumes",
        "short_range": "BCC crystals with random interstitial atom defects to capture "
        "short-range many-body dynamics",
        "surf_liquid": "Damaged and half-molten (110) and (100) surfaces",
        "tri-vacancy": "Trivacancy configurations",
    }
    # TODO fix
    cs_names = [
        "BCC_alloys",
        "liquid",
        "self_intersitial_defect",
        "vacancy",
        "BCC_distorted",
        "binary_BCC_alloys",
        "double_self_interstitial_defect",
        "divacancy",
        "dimer",
        "gamma_surface",
        "HEA_interstitial",
        "HEA_short_range",
        "HEA_small",
        "HEA_surface",
        "HEA_vacancies",
        "isolated_atom",
        "liquid_composition",
        "HEA_liquid",
        "MCMD",
        "ordered_alloys",
        "phonon",
        "short_range",
        "suface_liquid",
        "tri_vacancy",
    ]
    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=cs_names[i],
            description=desc,
            ds_id=ds_id,
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


"""
configuration_label_regexes = {
    'alloys':
        'bcc',
    'bcc':
        ['bcc', 'strain'],
    'bcc_distorted':
        ['bcc', 'strain'],
    'di-sia':
        'interstitial',
    'di-vacancy':
        ['vacancy', 'divacancy'],
    'dimer':
        ['dimer', 'warning', 'large_forces', 'repulsive'],
    'gamma_surface':
        'gamma_surface',
    'hea':
        'hea',
    'hea_ints':
        ['hea', 'interstitial'],
    'isolated_atom':
        'isolated_atom',
    'liquid':
        'liquid',
    'mcmd':
        'aimd',
    'phonon':
        'aimd',
    'short_range':
        ['bcc', 'interstitial', 'warning', 'large_forces', 'repulsive'],
    'sia':
        'interstitial',
    'surf_liquid':
        'surface',
    'surface':
        'surface',
    'tri-vacancy':
        ['vacancy', 'divacancy', 'trivacancy'],
    'vacancies':
        'vacancy',
    'vacancy':
        'vacancy',
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

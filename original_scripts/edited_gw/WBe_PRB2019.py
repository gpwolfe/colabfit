#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data


import os
import json
import numpy as np
from ase import Atoms

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/FitSNAP/"
    "examples/WBe_PRB2019/JSON/"
)
DATASET = "WBe_PRB2019"

LINKS = [
    "https://doi.org/10.1103/PhysRevB.99.184305",
    "https://github.com/FitSNAP/FitSNAP/tree/master/examples/WBe_PRB2019",
]
AUTHORS = ["M. A. Wood", "M. A. Cusentino", "B. D. Wirth", "A. P. Thompson"]
DS_DESC = (
    "This data set was originally used to generate a "
    "multi-component linear SNAP potential for tungsten and beryllium as "
    "published in Wood, M. A., et. al. Phys. Rev. B 99 (2019) 184305. This "
    "data set was developed for the purpose of studying plasma material "
    "interactions in fusion reactors."
)


def reader(file_path, **kwargs):
    with open(file_path) as f:
        data = json.loads("\n".join(f.readlines()[kwargs["header_lines"] :]))

    symbols = data["Dataset"]["Data"][0]["AtomTypes"]
    positions = np.array(data["Dataset"]["Data"][0]["Positions"])
    box = np.array(data["Dataset"]["Data"][0]["Lattice"])

    at_name = os.path.splitext(str(file_path).split("JSON")[1][1:])[0]

    try:
        atoms = Atoms(symbols, positions=positions, cell=box, pbc=[1, 1, 1])
    except:  # noqa: E722
        symbols = symbols[1:]
        atoms = Atoms(symbols, positions=positions, cell=box, pbc=[1, 1, 1])

    atoms.info["name"] = at_name
    atoms.info["energy"] = data["Dataset"]["Data"][0]["Energy"]
    atoms.arrays["forces"] = np.array(data["Dataset"]["Data"][0]["Forces"])

    atoms.info["stress"] = np.array(data["Dataset"]["Data"][0]["Stress"])

    atoms.info["per-atom"] = False

    yield atoms


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
            file_format="folder",
            name_field="name",
            elements=["W", "Be"],
            default_name=DATASET,
            reader=reader,
            glob_string="*.json",
            verbose=True,
            header_lines=1,
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
                "stress": {"field": "stress", "units": "GPa"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations, property_map=property_map, generator=False, verbose=True
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    cs_regexes = {
        "(001|010|100)FreeSurf": "Be [001], [010], and [100] surfaces",
        "DFTMD_1000K": "AIMD sampling of Be at 1000K",
        "DFTMD_300K": "AIMD sampling of Be at 300K",
        "DFT_MD_1000K": "AIMD sampling of W-Be at 1000K",
        "DFT_MD_300K": "AIMD sampling of W-Be at 300K",
        "Divacancy": "Divacancy defects in pure W",
        "^EOS": "Energy vs. volume EOS configurations for W-Be",
        "EOS_(BCC|FCC|HCP)": "Energy vs. volume EOS configurations for Be in various "
        "crystal structures",
        "EOS_Data": "Energy vs. volume configurations for W",
        "Elast_(BCC|FCC|HCP)_(Shear|Vol)": "BCC, FCC, and HCP Be with shear or "
        "volumetric strains",
        "ElasticDeform_(Shear|Vol)": "W-Be in various crystal structures with shear "
        "and volumetric strains",
        "Liquids": "Liquid Be",
        "StackFaults": "Be stacking faults",
        "WSurface_BeAdhesion": "Be adhesion onto W surfaces",
        "dislocation_quadrupole": "W dislocation quadrupole configurations",
        "^gamma_surface/": "W gamma surface configurations",
        "gamma_surface_vacancy": "W gamma surface configurations",
        "md_bulk": "AIMD sampling of bulk W",
        "^surface": "Pure W surfaces",
        "^vacancy": "Bulk W with vacancy defects",
    }

    cs_names = [
        "Be_surface",
        "AIMD_Be_1000K",
        "AIMD_Be_300K",
        "AIMD_W-Be_1000K",
        "AIMD_W-Be_300K",
        "W_divacancy",
        "EOS",
        "EOS_crystal",
        "EOS_data",
        "Be_shear_strain",
        "W-BE_shear_strain",
        "liquid",
        "stack_fault",
        "W-surface_Be_adhesion",
        "dislocation_quadrupole",
        "gamma_surface",
        "gamma_surface_vacancy",
        "MD_bulk",
        "surface",
        "vacancy",
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

    ds_id = client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )
    ds_id


"""
configuration_label_regexes = {
    "FreeSurf": "surface",
    "surface": "surface",
    "BCC": "bcc",
    "DFT(_)?MD": "aimd",
    "Divacancy": "divacancy",
    "EOS": "eos",
    "Elast": "elastic",
    "Liquids": "liquid",
    "StackFaults": "stacking_fault",
    "dislocation": "dislocation",
    "gamma_surface": "gamma_surface",
    "md_bulk": "aimd",
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

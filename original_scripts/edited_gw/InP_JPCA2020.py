#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
import json
import os
from pathlib import Path
import sys

import numpy as np

from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/FitSNAP/"
    "examples/InP_JPCA2020/JSON"
)
DATASET = "InP_JPCA2020"

LINKS = [
    "https://doi.org/10.1021/acs.jpca.0c02450",
    "https://github.com/FitSNAP/FitSNAP/tree/master/examples/InP_JPCA2020",
]
AUTHORS = ["Mary Alice Cusentino", "Mitchell A. Wood", "Aidan P. Thompson"]

DS_DESC = (
    "This data set was used to generate a multi-element "
    "linear SNAP potential for InP, as published in Cusentino, M. A. et. al, "
    "J. Chem. Phys. (2020). Intended to produce an interatomic potential for "
    "indium phosphide capable of capturing high-energy defects that result "
    "from radiation damage cascades."
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
    except Exception as e:
        print("Error on :", at_name, e, set(symbols))
        symbols = symbols[1:]
        atoms = Atoms(symbols, positions=positions, cell=box, pbc=[1, 1, 1])

    atoms.info["name"] = at_name
    atoms.info["energy"] = data["Dataset"]["Data"][0]["Energy"]
    atoms.arrays["forces"] = np.array(data["Dataset"]["Data"][0]["Forces"])

    atoms.info["stress"] = np.array(data["Dataset"]["Data"][0]["Stress"])

    atoms.info["per-atom"] = False
    # atoms.info['reference-energy'] = 3.48

    return [atoms]


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
            elements=["In", "P"],
            default_name=DATASET,
            reader=reader,
            glob_string="*.json",
            # verbose=True,
            header_lines=2,
        )
    )

    print(configurations[0])
    property_map = {
        "potential-energy": [
            {
                # ColabFit name: {'field': ASE field name, 'units': str}
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "reference-energy": {"field": "reference-energy", "units": "eV"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-LDA"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-LDA"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "kilobar"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-LDA"},
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
        "^Bulk": "Ground state configuration for bulk zinc blende",
        "^EOS": "Bulk zinc blende with uniform expansion and compression",
        "^Shear": "Bulk zincblende with random cell shape modifications",
        "^Strain": "Uniaxially strained bulk zinc blende",
        "^a(In|P)": "Antisite defects in InP",
        "^aa": "Diantisite defects",
        "^i(In|P)": "Interstitial defects in InP",
        "^vP": "Vacancy defects in InP",
        "^vv": "Divacancy defects in InP",
        "^s_a(In|P|a)": "No description",
        "^s_i(In|P)": "No description",
        "^s_v(In|P)": "No description",
        "^s_vv": "No description",
    }

    cs_ids = []
    # TODO fix
    cs_names = [
        "bulk",
        "EOS",
        "sheared",
        "strained",
        "antisite_defect",
        "diantisite_defect",
        "interstitial_defect",
        "vacancy_defect",
        "divacancy_defect",
        "other",
        "other",
        "other",
        "other",
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
    "Bulk|EOS|Shear|Strain": "zincblende",
    "EOS": "eos",
    "Shear|Strain": "strain",
    "^a(In|P)": "antisite",
    "^aa": "diantisite",
    "^i(In|P)": "interstitial",
    "^v(In|P|v)": "vacancy",
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

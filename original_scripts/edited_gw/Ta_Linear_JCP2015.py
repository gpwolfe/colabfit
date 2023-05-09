#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
import json
import os
import numpy as np
from pathlib import Path
import sys

from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data

DATASET = "Ta_Linear_JCP2015"

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/"
    "FitSNAP/examples/Ta_Linear_JCP2014/JSON/"
)


LINKS = [
    "https://www.sciencedirect.com/science/article/pii/S0021999114008353",
    "https://github.com/FitSNAP/FitSNAP/tree/master/examples/Ta_Linear_JCP2014",
]
AUTHORS = [
    "A. P. Thompson",
    "L. P. Swiler",
    "C. R. Trott",
    "S. M. Foiles",
    "G. J. Tucker",
]
DS_DESC = (
    "This data set was originally used to generate a "
    "linear SNAP potential for solid and liquid tantalum as published in "
    "Thompson, A.P. et. al, J. Comp. Phys. 285 (2015) 316-330."
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
            elements=["Ta"],
            default_name=DATASET,
            reader=reader,
            glob_string="*.json",
            # verbose=True,
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
                "stress": {"field": "virial", "units": "bar"},
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
            configurations, property_map=property_map, generator=False, verbose=True
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    len(set(all_co_ids))

    len(set(all_pr_ids))

    cs_regexes = {
        "Displaced_A15": "A15 configurations with random displacement of atomic "
        "positions",
        "Displaced_BCC": "BCC configurations with random displacement of atomic "
        "positions",
        "Displaced_FCC": "FCC configurations with random displacement of atomic "
        "positions",
        "Elastic_BCC": "BCC primitive cells with random strains",
        "Elastic_FCC": "FCC primitive cells with random strains",
        "GSF_(110|112)": "Relaxed and unrelaxed generalized stacking faults along the "
        "[110] and [112] crystallographic directions",
        "Liquid": "High-temperature AIMD sampling of molten tantalum",
        "Surface": "Relaxed and unrelaxed [100], [110], [111], and [112] BCC surfaces",
        "Volume_A15": "A15 primitive cells, compressed or expanded isotropically over "
        "a wide range of densities",
        "Volume_BCC": "BCC primitive cells, compressed or expanded isotropically over "
        "a wide range of densities",
        "Volume_FCC": "FCC primitive cells, compressed or expanded isotropically over "
        "a wide range of densities",
    }

    cs_names = [
        "displaced_A15",
        "displaced_BCC",
        "displaced_FCC",
        "elastic_BCC",
        "elastic_FCC",
        "GSF",
        "liquid",
        "surface",
        "volume_A15",
        "volume_BCC",
        "volume_FCC",
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
        "A15": "a15",
        "BCC": "bcc",
        "FCC": "fcc",
        "GSF": "stacking_fault",
        "Liquid": "liquid",
        "Surface": "surface",
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

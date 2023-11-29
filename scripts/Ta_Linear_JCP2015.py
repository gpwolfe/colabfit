#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
import json
import os
import numpy as np
from pathlib import Path
import sys

from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

DATASET = "Ta_Linear_JCP2014"

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/"
    "FitSNAP/examples/Ta_Linear_JCP2014/JSON/"
)
DATASET_FP = Path().cwd().parent / "data/FitSNAP-master/examples/Ta_Linear_JCP2014/JSON"

PUBLICATION = "https://doi.org/10.1016/j.jcp.2014.12.018"
DATA_LINK = "https://github.com/FitSNAP/FitSNAP/tree/master/examples/Ta_Linear_JCP2014"
LINKS = [
    "https://doi.org/10.1016/j.jcp.2014.12.018",
    "https://github.com/FitSNAP/FitSNAP/tree/master/examples/Ta_Linear_JCP2014",
]
AUTHORS = [
    "Aidan P. Thompson",
    "Laura P. Swiler",
    "Christian R. Trott",
    "Stephen M. Foiles",
    "Garritt J. Tucker",
]
DS_DESC = (
    "This data set was originally used to generate a "
    "linear SNAP potential for solid and liquid tantalum as published in "
    "Thompson, A.P. et. al, J. Comp. Phys. 285 (2015) 316-330."
)

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "encut": {"value": 500},
    "k-point-scheme": {"value": "Monkhorst-Pack"},
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
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "bar"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


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

    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

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
            ds_id=ds_id,
            query={"names": {"$regex": regex}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
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

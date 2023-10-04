#!/usr/bin/env python
# coding: utf-8
"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
potential energy
atomic forces

File notes
----------
xyz header:
Lattice
Properties=species:S:1:pos:R:3:forces:R:3
energy=-206.92580857
pbc="F F F"
"""

from argparse import ArgumentParser
from ase.io import read
from pathlib import Path
import sys

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/new_raw_datasets_2.0/a-AlOx/")
DATASET_FP = Path().cwd().parent / "data/a-AlOx"
DS_NAME = "a-AlOx_JCP_2020"
DS_DESC = (
    "In this work, the ab initio calculations were performed"
    "with the Vienna Ab initio Simulation Package. The projector"
    "augmented wave method was used to treat the atomic core electrons,"
    "and the Perdew–Burke–Ernzerhof functional within the generalized "
    "gradient approximation was used to describe the electron–electron "
    "interactions.The cutoff energy for the plane-wave basis set was "
    "set to 550 eV during the ab initio calculation.The structural, "
    "vibrational, mechanical, and thermal properties of the "
    "a-AlOx models were investigated. Finally, the obtained reference "
    "database includes the DFT energies of 41 203 structures. "
    "The supercell size of the AlOx reference structures varied from 24 to 132 atoms",
)
AUTHORS = ["Wenwen Li", "Yasunobu Ando", "Satoshi Watanabe"]
LINKS = [
    "https://doi.org/10.1063/5.0026289",
    "https://doi.org/10.24435/materialscloud:y1-zd",
]
GLOB = "*.xyzdat"

PI_MD = {
    "software": {"value": "CASTEP"},
    "method": {"value": "DFT-PW91"},
    "energy-cutoff": {"value": "250 eV"},
    "temperature": {"field": "temperature"},
    "kpoints": {"field": "kpoints"},
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_MD,
        }
    ],
}


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "PBE"},
                "ecut": {"value": "550 eV"},
                "kpoint": {"value": "5×5×2 or 5×3×2"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "PBE"},
                "ecut": {"value": "550 eV"},
                "kpoint": {"value": "5×5×2 or 5×3×2"},
            },
        }
    ],
}


def reader(fp):
    configs = read(fp, index=":")
    return configs


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

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=["Al", "O"],
        verbose=True,
        generator=False,
        glob_string=GLOB,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    css = [
        (
            "train",
            "a-AlOx_training",
            "Structures used for training of neural network potential.",
        ),
        (
            "additional",
            "a-AlOx_reference",
            "Additional reference DFT calculations that author used for reference.",
        ),
    ]
    cs_ids = []
    for i, (reg, name, desc) in enumerate(css):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": reg}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        cs_ids=cs_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

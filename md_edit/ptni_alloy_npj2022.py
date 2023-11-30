#!/usr/bin/env python
# coding: utf-8
"""
File notes
-------------
Script uses 'ecut' argument in property definitions. If this is not generally
implemented, consider moving to metadata
"""
from argparse import ArgumentParser
from pathlib import Path
import sys


from ase.db import connect
from tqdm import tqdm
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)


DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/PtNi_alloy_dft_Han")
DATASET_FP = Path().cwd().parent / "data/ptni_alloy_npj_2022"
DATASET = "PtNi_alloy_NPJ2022"

PUBLICATION = "https://doi.org/10.1038/s41524-022-00807-6"
DATA_LINK = "https://zenodo.org/record/5645281#.Y2CPkeTMJEa"
LINKS = [
    "https://doi.org/10.1038/s41524-022-00807-6",
    "https://zenodo.org/record/5645281#.Y2CPkeTMJEa",
]
AUTHORS = [
    "Shuang Han",
    "Giovanni Barcaro",
    "Alessandro Fortunelli",
    "Steen Lysgaard",
    "Tejs Vegge",
    "Heine Anton Hansen",
]

DS_DESC = (
    "DFT dataset consisting of 6828 resampled Pt-Ni alloys used for training an "
    "NNP. The energy and forces of each structure in the resampled database are "
    "calculated using DFT. All reference DFT calculations for the training set of "
    "6828 Pt-Ni alloy structures have been performed using the Vienna Ab initio "
    "Simulation Package (VASP) with the spin-polarized revised Perdew-Burke-Ernzerhof "
    "(rPBE) exchange-correlation functional."
)
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-rPBE"},
    "encut": {"value": "500 eV"},
    "ismear": {"value": 0},
    "sigma": {"value": 0.1},
    "ediff": {"value": "1x10e^-6"},
    "k-points": {"value": "500/Ang^-3 of reciprocal cell, Monkhorst-Pack"},
}
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "potential_energy", "units": "eV"},
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
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
}


def tform(c):
    c.info["per-atom"] = False


def reader_PtNi(p):
    atoms = []
    db = connect(p)
    for i in tqdm(range(1, 6829)):
        atom = db.get_atoms(i)
        atom.info["potential_energy"] = atom.calc.results["energy"]
        free_energy = atom.calc.results.get("free_energy")
        if free_energy:
            atom.info["free_energy"] = free_energy
        atom.arrays["forces"] = atom.calc.results["forces"]
        atom.info["per-atom"] = False
        atoms.append(atom)
    return atoms


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
    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(potential_energy_pd)
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=["Pt", "Ni"],
        default_name="PtNi",
        reader=reader_PtNi,
        glob_string="*.db",
        verbose=True,
        generator=False,
    )

    """
    free_property_definition = {
        "property-id": "free-energy",
        "property-name": "free-energy",
        "property-title": "free energy",
        "property-description": "the amount of internal energy of a thermodynamic "
        "system that is available to perform work",
        "energy": {
            "type": "float",
            "has-unit": True,
            "extent": [],
            "required": True,
            "description": "the amount of internal energy of a thermodynamic system "
            "that is available to perform work",
        },
    }
    """
    # client.insert_property_definition(free_property_definition)

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

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

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
from colabfit.tools.database import MongoDatabase, load_data


DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/PtNi_alloy_dft_Han")
DATASET = "PtNi_alloy_NPJ2022"


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

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

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-rPBE"},
                    "ecut": {"value": "500 eV"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-rPBE"},
                    "ecut": {"value": "500 eV"},
                },
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "free_energy", "units": "eV"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-rPBE"},
                    "ecut": {"value": "500 eV"},
                },
            }
        ],
    }

    def tform(c):
        c.info["per-atom"] = False

    ids = list(
        client.insert_data(
            configurations,
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
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

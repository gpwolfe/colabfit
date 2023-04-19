#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from tqdm import tqdm
from pathlib import Path
import pickle
import sys

DATASET_FP = Path("/large_data/new_raw_datasets/ABC2D6-16/abc2d6-16/")
SCRIPT_FP = Path("/home/ubuntu/calc_notebooks/*.json")
DATASET_NAME = "ABC2D6-16_PRL2018"
AUTHORS = [
    "F. Faber",
    "A. Lindmaa",
    "O. A. v. Lilienfeld",
    "R. Armiento",
]
LINKS = [
    "https://journals.aps.org/prl/abstract/10.1103/PhysRevLett.117.135502",
    "https://arxiv.org/abs/1508.05315" "https://qmml.org/datasets.html",
]
DESCRIPTION = "Dataset used to train a machine learning model to calculate\
 density functional theory-quality formation energies of all ∼2 × 106 pristine\
 ABC2D6 elpasolite crystals that can be made up from main-group elements (up\
 to bismuth)."

ELEMENTS = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Po",
    "At",
    "Rn",
    "Fr",
    "Ra",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
    "Am",
    "Cm",
    "Bk",
    "Cf",
    "Es",
    "Fm",
    "Md",
    "No",
    "Lr",
    "Rf",
    "Db",
    "Sg",
    "Bh",
    "Hs",
    "Mt",
    "Ds",
    "Rg",
    "Cn",
    "Uut",
    "Uuq",
    "Uup",
    "Uuh",
    "Uus",
    "Uuo",
]


def reader_ABC(p):
    atoms = []
    f = open(p, "rb")
    a = pickle.load(f, encoding="latin1")
    a["T"] = a["T"] / a["N"]
    for i in tqdm(range(len(a["T"]))):
        atom = Atoms(
            numbers=a["Z"][i],
            scaled_positions=a["Co"][i],
            cell=a["Ce"][i],
            pbc=[1, 1, 1],
        )
        atom.info["formation_energy"] = a["T"][i]
        atom.info["per-atom"] = True
        atom.info["representation"] = a["X"][i]
        atoms.append(atom)
    return atoms


def tform(c):
    c.info["per-atom"] = True


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
        elements=ELEMENTS,
        default_name="ElpasoliteIIItoVI",
        reader=reader_ABC,
        glob_string="ElpasoliteIIItoVI.pkl",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=ELEMENTS,
        default_name="TrainingSet",
        reader=reader_ABC,
        glob_string="TrainingSet.pkl",
        verbose=True,
        generator=False,
    )

    pds = SCRIPT_FP.glob("*.json")
    for pd in pds:
        client.insert_property_definition(pd)

    # TODO actually formation energy
    property_map = {
        "formation-energy": [
            {
                "energy": {"field": "formation_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                # For metadata want: software, method (DFT-XC Functional)
                # basis information, more generic parameters
                "_metadata": {
                    "software": {"value": "VASP"},  # ex. VASP
                    "method": {"value": "DFT-PBE"},  # ex. PBE-MBD
                    # Kmesh 3x3x3 energy i
                },
            }
        ]
    }

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

    # matches to data CO "name" field
    # TODO finish
    cs_regexes = {
        "ElpasoliteIIItoVI_.*": "Data obtained from ElpasoliteIItoVI.pkl",
        "TrainingSet_.*": "Data obtained from TrainingSet.pkl",
    }

    cs_names = [
        "ElpasoliteIIItoVI",
        "TrainingSet",
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
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        # for the description I just put the abstract...
        # TODO I want to change this to something associated with the data
        description=DESCRIPTION,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

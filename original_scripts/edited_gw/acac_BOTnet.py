#!/usr/bin/env python
# coding: utf-8

from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/BOTNet")
SCRIPT_FP = Path("")
DATASET_NAME = "BOTnetACAC_arXiv2022"
AUTHORS = [
    "I. Batatia",
    "S. Batzner",
    "D. P. Kovacs",
    "A. Musaelian",
    "G. N. C. Simm",
    "R. Drautz",
    "C. Ortner",
    "B. Kozinsky",
    "G. Csanyi",
]
LINKS = [
    "https://arxiv.org/abs/2205.06643",
    "https://github.com/davkovacs/BOTNet-datasets",
]
DESCRIPTION = (
    "Acetylacetone dataset generated from "
    "a long molecular dynamics simulation at 300 K using a "
    "Langevin thermostat at the semi-empirical GFN2-xTB "
    "level of theory. Configurations were sampled at an "
    "interval of 1 ps and the resulting set of configurations were recomputed with "
    "density functional theory using the PBE "
    "exchange-correlation functional with D3 dispersion correction and def2-SVP "
    "basis set and VeryTightSCF convergence settings using the ORCA electronic "
    "structure package."
)

ELEMENTS = ["C", "H", "O"]


def tform(c):
    c.info["per-atom"] = False


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
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    # Loads data, specify reader function if not "usual" file format
    configurations = load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/isolated_atoms.xyz",
        file_format="extxyz",
        name_field=None,
        elements=[
            "C",
            "H",
            "O",
        ],
        default_name="isolated",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/test_H_transfer.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="H_transfer",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/test_MD_300K.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="test_300K_MD",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/test_MD_600K.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="test_600K_MD",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/test_dihedral.xyz",
        file_format="xyz",
        name_field=None,
        elements=[
            "C",
            "H",
            "O",
        ],
        default_name="Dihedral_scan",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/train_300K.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="train_300K_MD",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "BOTNet-datasets-main/dataset_acac/train_600K.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="train_600K_MD",
        verbose=True,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "ORCA 5.0"},
                    "method": {"value": "DFT-PBE+D3"},
                    "basis-type": {"def2-SVP"},
                    # TODO need to change this to the readme data for acac
                    # TODO Orca 5.0, density functional theory, PBE exchange
                    # correlation functional, def2-SVP basis set, VeryTightSCF,
                    # Grid 7, NoFinalGrid. For the dissociation curve the DFT
                    # calculation uses open shellUHF-PBE and basin hopping to
                    # find the lowest energy dissociation curve.
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "ORCA 5.0"},
                    "method": {"value": "DFT-PBE+D3"},
                    "basis-type": {"def2-SVP"},
                    # Orca 5, density functional theory, PBE exchange
                    # correlation functional, def2-SVP basis set,
                    # VeryTightSCF, Grid 7, NoFinalGrid
                    # For the dissociation curve the DFT calculation uses
                    # open shell UHF-PBE and basin hopping to find the
                    # lowest energy dissociation curve.
                },
            }
        ],
    }
    # for c in configurations:
    #     c.info["per-atom"] = False
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
    cs_regexes = {
        # .*, is there a way to check all names,
        # maybe with that python stuff eric was talking about. plan:
        # finish this without csregexes->try to upload->
        # see if grabbed cs regexes show up so i can just take the names from
        # there
        "isolated": "Energies of the isolated atoms evalauted at the "
        "reference DFT settings",
        "H_transfer": "NEB path of proton transfer reaction between the two"
        " forms of the molecule",
        "test_300K_MD": "Test set of decorrelated geometries sampled from 300"
        " K xTB MD",
        "test_600K_MD": "Test set of decorrelated geometries sampled from 600"
        " K xTB MD",
        "Dihedral_scan": "Dihedral scan about one of the C-C bonds of the"
        " conjugated system",
        "train_300K_MD": "500 decorrelated geometries sampled from 300 K xTB" " MD run",
        "train_600K_MD": "500 decorrelated geometries sampled from 600 K xTB" " MD run",
    }

    cs_names = [
        "isolated",
        "H_transfer",
        "test_300K_MD",
        "test_600K_MD",
        "Dihedral_scan",
        "train_300K_MD",
        "train_600K_MD",
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
        description=DESCRIPTION,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
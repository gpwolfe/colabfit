#!/usr/bin/env python
# coding: utf-8

from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets/"
    "BOTNet/BOTNet-datasets-main/dataset_acac"
)
DATASET_FP = Path().cwd().parents / "data/dataset_acac"  # local
DATASET_NAME = "BOTnet_ACAC_2022"
AUTHORS = [
    "Ilyes Batatia",
    "Simon Batzner",
    "Dávid Péter Kovács",
    "Albert Musaelian",
    "Gregor N. C. Simm",
    "Ralf Drautz",
    "Christoph Ortner",
    "Boris Kozinsky",
    "Gábor Csányi",
]
PUBLICATION = "https://doi.org/10.48550/arXiv.2205.06643"
DATA_LINK = "https://github.com/davkovacs/BOTNet-datasets"
LINKS = [
    "https://doi.org/10.48550/arXiv.2205.06643",
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    # Loads data, specify reader function if not "usual" file format

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "ORCA 5.0"},
                    "method": {"value": "DFT-PBE-D3"},
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
                    "method": {"value": "DFT-PBE-D3"},
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
    # filepath, description, config default name
    glob_dss = [
        (
            "isolated_atoms.xyz",
            "Energies of the isolated atoms evalauted at the reference DFT settings. "
            f"{DESCRIPTION}",
            "isolated",
        ),
        (
            "test_H_transfer.xyz",
            "NEB path of proton transfer reaction between the two forms of "
            f"acetylacetone. {DESCRIPTION}",
            "H_transfer",
        ),
        (
            "test_MD_300K.xyz",
            "Test set of decorrelated geometries sampled from 300"
            f" K xTB MD. {DESCRIPTION}",
            "test_300K_MD",
        ),
        (
            "test_MD_600K.xyz",
            "Test set of decorrelated geometries sampled from 600 K xTB MD. "
            f"{DESCRIPTION}",
            "test_600K_MD",
        ),
        (
            "test_dihedral.xyz",
            "Dihedral scan about one of the C-C bonds of the conjugated system. "
            f"{DESCRIPTION}",
            "Dihedral_scan",
        ),
        (
            "train_300K.xyz",
            f"500 decorrelated geometries sampled from 300 K xTB MD run. {DESCRIPTION}",
            "train_300K_MD",
        ),
        (
            "train_600K.xyz",
            f"500 decorrelated geometries sampled from 600 K xTB MD run. {DESCRIPTION}",
            "train_600K_MD",
        ),
    ]
    for glob_ds in glob_dss:
        configurations = load_data(
            file_path=DATASET_FP / glob_ds[0],
            file_format="xyz",
            name_field=None,
            elements=ELEMENTS,
            default_name=glob_ds[2],
            verbose=True,
            generator=False,
        )
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

        # # matches to data CO "name" field
        # cs_regexes = {
        #     "isolated": "Energies of the isolated atoms evalauted at the "
        #     "reference DFT settings",
        #     "H_transfer": "NEB path of proton transfer reaction between the two"
        #     " forms of the molecule",
        #     "test_300K_MD": "Test set of decorrelated geometries sampled from 300"
        #     " K xTB MD",
        #     "test_600K_MD": "Test set of decorrelated geometries sampled from 600"
        #     " K xTB MD",
        #     "Dihedral_scan": "Dihedral scan about one of the C-C bonds of the"
        #     " conjugated system",
        #     "train_300K_MD": "500 decorrelated geometries sampled from 300 K xTB"
        #       " MD run",
        #     "train_600K_MD": "500 decorrelated geometries sampled from 600 K xTB"
        #       " MD run",
        # }

        # cs_names = [
        #     "isolated",
        #     "H_transfer",
        #     "test_300K_MD",
        #     "test_600K_MD",
        #     "Dihedral_scan",
        #     "train_300K_MD",
        #     "train_600K_MD",
        # ]

        # cs_ids = []

        # for i, (regex, desc) in enumerate(cs_regexes.items()):
        #     cs_id = client.query_and_insert_configuration_set(
        #         co_hashes=all_co_ids,
        #         name=cs_names[i],
        #         description=desc,
        #         query={"names": {"$regex": regex}},
        #     )
        #     cs_ids.append(cs_id)

        client.insert_dataset(
            # cs_ids=cs_ids,
            do_hashes=all_pr_ids,
            name=f"{DATASET_NAME}_{glob_ds[2]}",
            ds_id=ds_id,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=glob_ds[1],
            resync=True,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

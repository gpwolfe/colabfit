#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from ase.atoms import Atoms

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/sGDML")
# DATASET_FP = Path().cwd().parents[1] / "scripts/data/benzene_ccsd_t"  # remove
DATASET = "Benzene_ccsdt_NC2018"

PUBLICATION = "https://doi.org/10.1038/s41467-018-06169-2"
DATA_LINK = "http://sgdml.org/"
LINKS = [
    "https://doi.org/10.1038/s41467-018-06169-2",
    "http://sgdml.org/",
]
AUTHORS = [
    "Stefan Chmiela",
    "Huziel E. Sauceda",
    "Klaus-Robert Müller",
    "Alexandre Tkatchenko",
]
DS_DESC = (
    "To create the coupled cluster datasets, the data used "
    "for training the models were created by running ab initio MD in the "
    "NVT ensemble using the Nosé-Hoover thermostat at 500 K during a 200 ps "
    "simulation with a resolution of 0.5 fs. Energies and forces were "
    "recalculated using all-electron coupled cluster with single "
    ", double and perturbative triple excitations (CCSD(T)). The Dunning "
    "correlation-consistent basis set cc-pVDZ was used for benzene. All "
    "calculations were performed with the Psi4 software suite."
)


def tform(c):
    c.info["per-atom"] = False


# sGDML->checkout README
def reader_sGDML(filepath):
    with open(filepath, "r") as f:
        configs = []
        lines = f.readlines()
        while len(lines) > 0:
            symbols = []
            positions = []
            forces = []
            natoms = int(lines.pop(0))
            energy = float(lines.pop(0))  # Comment line; ignored
            for _ in range(natoms):
                line = lines.pop(0)
                symbol = line.split()[0]
                positions.append([float(p) for p in line.split()[1:4]])
                forces.append([float(f) for f in line.split()[4:]])
                symbol = symbol.lower().capitalize()
                symbols.append(symbol)
            config = Atoms(symbols=symbols, positions=positions)
            config.info["energy"] = energy
            config.info["forces"] = forces
            configs.append(config)
    for i, a in enumerate(configs):
        a.info["per-atom"] = False
        a.info["_name"] = f"{filepath.stem}_{i}"
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "Psi4"},
                    "method": {"value": "CCSD(T)"},
                    "basis": {"value": "cc-pVDZ"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/molAng"},
                "_metadata": {
                    "software": {"value": "Psi4"},
                    "method": {"value": "CCSD(T)"},
                    "basis": {"value": "cc-pVDZ"},
                },
            }
        ],
    }

    for train_test in ["train", "test"]:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="_name",
            elements=["C", "H"],
            reader=reader_sGDML,
            glob_string=f"benzene_ccsd_t-{train_test}.xyz",
            default_name=f"benzene_ccsd_t-{train_test}",
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

        client.insert_dataset(
            # cs_ids=cs_ids,
            do_hashes=all_pr_ids,
            ds_id=ds_id,
            name=f"{DATASET}_{train_test}",
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=(
                f"The {train_test} set of a train/test pair from the benzene "
                f"dataset from sGDML. {DS_DESC}"
            ),
            resync=True,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

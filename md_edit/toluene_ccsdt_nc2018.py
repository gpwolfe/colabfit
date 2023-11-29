#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.atoms import Atoms
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/sGDML")
# DATASET_FP = Path().cwd().parents[1] / "scripts/data/toluene_ccsd_t"  # remove

DATASET = "Toluene_ccsdt_NC2018"

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
    "recalculated using all-electron coupled cluster with single, "
    "double and perturbative triple excitations (CCSD(T)). "
    "The Dunning correlation-consistent basis set cc-pVDZ was "
    "used for malonaldehyde. All calculations were performed with the Psi4 software "
    "suite."
)
PI_MD = {
    "software": {"value": "Psi4"},
    "method": {"value": "CCSD(T)"},
    "basis-set": {"value": "cc-pVDZ"},
}
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "kcal/mol"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "kcal/molAng"},
            "_metadata": PI_MD,
        }
    ],
}


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

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    for train_test in ["train", "test"]:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="_name",
            elements=["H", "C"],
            reader=reader_sGDML,
            glob_string=f"toluene_ccsd_t-{train_test}.xyz",
            default_name=f"toluene_ccsd_t-{train_test}",
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

        # cs_regexes = {
        #     "train": "Configurations used in training",
        #     "test": "Configurations used for testing",
        # }

        # cs_names = ["train", "test"]

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
            name=f"{DATASET}_{train_test}",
            authors=AUTHORS,
            ds_id=ds_id,
            links=LINKS,
            description=(
                f"The {train_test} set of a train/test pair from the "
                f"toluene dataset from sGDML. {DS_DESC}"
            ),
            resync=True,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

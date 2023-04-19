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
from ase.io import read

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets"
)

DATASET_NAME = "Aspirin_ccsd_NC2018"
AUTHORS = [
    "Stefan Chmiela",
    "Huziel E. Sauceda",
    "Klaus-Robert Müller",
    "Alexandre Tkatchenko",
]
LINKS = [
    "https://www.nature.com/articles/s41467-018-06169-2",
    "http://sgdml.org/",
]
DESCRIPTION = "To create the coupled cluster datasets, "
"the data used for training the models were created by running "
"abinitio MD in the NVT ensemble using the Nosé-Hoover thermostat "
"at 500 K during a 200 ps simulation with a resolution of 0.5 fs. "
"Energies and forces were recalculated by all-electron coupled cluster "
"with single, double and perturbative triple excitations (CCSD(T)). "
"The Dunning correlation-consistent basis set CCSD/cc-pVDZ was used "
"for aspirin. All calculations were performed with the Psi4 software suite."


def reader_sGDML(p):
    s = str(p).split("/")
    atoms = read(p, index=":", forces=True)
    for i, a in enumerate(atoms):
        a.info["energy"] = float(list(a.info.keys())[0])
        a.info["per-atom"] = False
        a.info["_name"] = "%s_%s" % (s[-1].split(".")[0], i)
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
        file_path="/large_data/new_raw_datasets/sGDML",
        file_format="folder",
        name_field="_name",
        elements=["O", "H", "C"],
        reader=reader_sGDML,
        glob_string="aspirin_ccsd-test.xyz",
        default_name="aspirin_ccsd-test",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets/sGDML",
        file_format="folder",
        name_field="_name",
        elements=["O", "H", "C"],
        reader=reader_sGDML,
        glob_string="aspirin_ccsd-train.xyz",
        default_name="aspirin_ccsd-train",
        verbose=True,
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "Psi4"},
                    "method": {"value": "CCSD"},
                    "basis": {"value": "cc-pVDZ"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/molAng"},
                "_metadata": {
                    "software": {"value": "Psi4"},
                    "method": {"value": "CCSD"},
                    "basis": {"value": "cc-pVDZ"},
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

    cs_regexes = {
        "train": "Configurations used in training",
        "test": "Configurations used for testing",
    }
    cs_names = ["train", "test"]

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

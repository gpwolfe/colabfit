#!/usr/bin/env python
# coding: utf-8
"""
Script notes:
Property definitions from colabfit-tools were not imported;
double-check to make sure these are working properly
"""

from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    free_energy_pd,
    potential_energy_pd,
)

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets/AlNiCu_Berk/AlNiCu_pruned.extxyz"
)
DATASET_FP = Path().cwd().parent / "data/alnicu_2020/AlNiCu_pruned.extxyz"

DATASET_NAME = "AlNiCu_AIP_2020"
AUTHORS = [
    "Berk Onat",
    "Christoph Ortner",
    "James R. Kermode",
]

PUBLICATION = "https://doi.org/10.1063/5.0016005"
DATA_LINK = "https://github.com/DescriptorZoo/sensitivity-dimensionality-results"
LINKS = [
    "https://doi.org/10.1063/5.0016005",
    "https://github.com/DescriptorZoo/sensitivity-dimensionality-results",
]

DESCRIPTION = (
    "This dataset is formed from two parts: single-species "
    "datasets for Al, Ni, and Cu from the NOMAD Encyclopedia and multi-species "
    "datasets that include Al, Ni and Cu from NOMAD Archive. Duplicates have been "
    "removed from NOMAD Encyclopedia data. For the multi-species data, only "
    "the last configuration steps for each NOMAD Archive record were used because the "
    "last configuration typically cooresponds with a fully relaxed configuration. "
    "In this dataset, the NOMAD unique reference access IDs are retained along "
    "with a subset of their meta information that includes whether the supplied "
    "configuration is from a converged calculation as well as the "
    "Density Functional Theory (DFT) code, version, and type of DFT functionals "
    "with the total potential energies. This dataset consists of 39.1% Al, "
    "30.7% Ni, and 30.2% Cu and has 27,987 atomic environments in 3337 structures."
)


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

    # Loads data, specify reader function if not "usual" file format
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="extxyz",
        name_field=None,
        elements=["Al", "Ni", "Cu"],
        default_name="AlNiCuData",
        verbose=False,
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)

    property_map = {  # TODO finish
        "potential-energy": [
            {
                "energy": {"field": "nomad_total_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"field": "nomad_program_name"},
                    "method": {"value": "DFT"},
                },
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "nomad_free_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"field": "nomad_program_name"},
                    "method": {"value": "DFT"},
                },
            }
        ],
    }

    for c in configurations:
        if "nomad_potential_energy" in c.info:
            c.info["per-atom"] = False
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET_NAME,
        authors=AUTHORS,
        ds_id=ds_id,
        links=[PUBLICATION, DATA_LINK],
        description=DESCRIPTION,
        resync=True,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

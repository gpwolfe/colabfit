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

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    free_energy_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets")

DATASET_NAME = "AlNiCu_AIP2020"
AUTHORS = ["B. Onat", "C. Ortner", "J. R. Kermode"]
LINKS = [
    "https://aip.scitation.org/doi/10.1063/5.0016005",
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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    # Loads data, specify reader function if not "usual" file format
    configurations = load_data(
        file_path=DATASET_FP / "AlNiCu_Berk/AlNiCu_pruned.extxyz",
        file_format="extxyz",
        name_field=None,
        elements=["Al", "Ni", "Cu"],
        default_name="AlNiCuData",
        verbose=True,
        generator=False,
    )

    # client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
    # client.insert_property_definition('/homei/ubuntu/notebooks/atomic-forces.json')
    # client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')

    # TODO this description is specific to the nomad encyclopedia description
    # for total and free energy. Eric you said you were going through and
    # checking property definitions we were writing, I dont
    # know what to write for a general description of total and free energy,
    # this might be something that needs to be edited.
    """
    total_energy_property_definition = {
        "property-id": "total-energy",
        "property-name": "total-energy",
        "property-title": "total energy",
        "property-description": "The energies are extracted from a "
        "representative calculation:"
        "for geometry optimization it is the last step of the optimization",
        "energy": {
            "type": "float",
            "has-unit": True,
            "extent": [],
            "required": True,
            "description": "total energy (not sure if per atom or unit cell)",
        },
    }

    client.insert_property_definition(total_energy_property_definition)

    free_energy_property_definition = {
        "property-id": "free-energy",
        "property-name": "free-energy",
        "property-title": "free energy",
        "property-description": "The energies are extracted from a "
        "representative calculation:"
        "for geometry optimization it is the last step of the optimization",
        "energy": {
            "type": "float",
            "has-unit": True,
            "extent": [],
            "required": True,
            "description": "free energy (not sure if per atom or unit cell)",
        },
    }
    """
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)

    property_map = {  # TODO finish
        #    'potential-energy': [{
        #        'energy':   {'field': 'nomad_potential_energy',
        #                     'units': 'eV'},
        #        'per-atom': {'field': 'per-atom', 'units': None},
        #        '_metadata': {
        #            'software': {'field':'nomad_program_name'},
        #        }
        #    }],
        "potential-energy": [
            {
                "energy": {"field": "nomad_total_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"field": "nomad_program_name"},
                },
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "nomad_free_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"field": "nomad_program_name"},
                },
            }
        ],
    }

    for c in configurations:
        if "nomad_potential_energy" in c.info:
            c.info["per-atom"] = False

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
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

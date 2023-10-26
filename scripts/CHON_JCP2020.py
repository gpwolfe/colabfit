#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/"
    "new_raw_datasets/CHON_berk/CHON.extxyz"
)
DATASET = "CHON_JCP2020"
PUBLICATION = "https://doi.org/10.1063/5.0016005"
DATA_LINK = (
    "https://github.com/DescriptorZoo/sensitivity-dimensionality-results/tree"
    "/master/datasets"
)

LINKS = [
    "https://doi.org/10.1063/5.0016005",
    "https://github.com/DescriptorZoo/sensitivity-dimensionality-results/tree"
    "/master/datasets",
]
AUTHORS = ["Berk Onat", "Christoph Ortner", "James R. Kermode"]
DS_DESC = (
    "This dataset of molecular structures was extracted, using the NOMAD API, "
    "from all available structures in the NOMAD Archive that only include C, H, "
    "O, and N. This dataset consists of 50.42% H, 30.41% C, 10.36% N, and "
    "8.81% O and includes 96 804 atomic environments in 5217 structures."
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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="extxyz",
        name_field=None,
        elements=["C", "H", "O", "N"],
        default_name="CHON",
        verbose=True,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "nomad_total_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": {
                    "software": {"field": "nomad_program_name"},
                    "method": {"field": "nomad_electronic_structure_method"},
                    "method_functional": {"field": "nomad_XC_functionals"},
                },
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "nomad_free_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": {
                    "software": {"field": "nomad_program_name"},
                    "method": {"field": "nomad_electronic_structure_method"},
                    "method_functional": {"field": "nomad_XC_functionals"},
                },
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/gubaev/CuPd/train.cfg"
)
DATASET = "CuPd_CMS2019"

LINKS = [
    "https://doi.org/10.1016/j.commatsci.2018.09.031",
    "https://gitlab.com/kgubaev/accelerating-high-throughput"
    "-searches-for-new-alloys-with-active-learning-data",
]
AUTHORS = ["K. Gubaev", "E. V. Podryabinkin", "G. L. W. Hart", "A. V. Shapeev"]
DS_DESC = (
    "This dataset was generated using the following active "
    "learning scheme: 1) candidate structures were relaxed by a partially-"
    "trained MTP model, 2) structures for which the MTP had to perform "
    "extrapolation were passed to DFT to be re-computed, 3) the MTP was "
    "retrained, including the structures that were re-computed with DFT, 4)"
    " steps 1-3 were repeated until the MTP no longer extrapolated on any of "
    "the original candidate structures. "
    "The original candidate structures for this dataset included "
    "40,000 unrelaxed configurations with BCC, FCC, and HCP lattices."
)

name = "CuPd_CMS2019"


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="cfg",
        name_field=None,
        elements=["Cu", "Pd"],
        default_name=name,
        verbose=True,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "GPa"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                },
            }
        ],
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
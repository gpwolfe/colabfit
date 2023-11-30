#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id

from colabfit.tools.property_definitions import (
    cauchy_stress_pd,
    potential_energy_pd,
    atomic_forces_pd,
)

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/gubaev/CuPd/train.cfg"
)
DATASET_FP = Path().cwd().parent / "data/CuPd/train.cfg"
DATASET = "CuPd_CMS2019"
PUBLICATION = "https://doi.org/10.1016/j.commatsci.2018.09.031"
DATA_LINK = (
    "https://gitlab.com/kgubaev/accelerating-high-throughput"
    "-searches-for-new-alloys-with-active-learning-data"
)
LINKS = [
    "https://doi.org/10.1016/j.commatsci.2018.09.031",
    "https://gitlab.com/kgubaev/accelerating-high-throughput"
    "-searches-for-new-alloys-with-active-learning-data",
]
AUTHORS = [
    "Konstantin Gubaev",
    "Evgeny V. Podryabinkin",
    "Gus L.W. Hart",
    "Alexander V. Shapeev",
]
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="cfg",
        name_field=None,
        elements=["Cu", "Pd"],
        default_name=DATASET,
        verbose=True,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "GPa"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT"},
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
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

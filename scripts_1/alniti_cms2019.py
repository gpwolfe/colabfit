#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/data/gubaev/AlNiTi/")
DATASET_FP = Path().cwd().parent / "data/alniti"
DATASET = "AlNiTi_CMS_2019"

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
    "retrained, including the structures that were re-computed with DFT, 4) "
    "steps 1-3 were repeated until the MTP no longer extrapolated on any of "
    "the original candidate structures. The original candidate structures "
    "for this dataset included about 375,000 binary and ternary structures, "
    "enumerating all possible unit cells with different symmetries (BCC, "
    "FCC, and HCP) and different number of atoms."
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

    configurations = load_data(
        file_path=DATASET_FP / "train_1st_stage.cfg",
        file_format="cfg",
        name_field=None,
        elements=["Al", "Ni", "Ti"],
        default_name="train_1st_stage",
        verbose=False,
        generator=False,
    )

    configurations += load_data(
        file_path=DATASET_FP / "train_2nd_stage.cfg",
        file_format="cfg",
        name_field=None,
        elements=["Al", "Ni", "Ti"],
        default_name="train_2nd_stage",
        verbose=False,
        generator=False,
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
                "forces": {"field": "forces", "units": "eV/angstrom"},
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

    for c in configurations:
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

    cs_regexes = {
        "train_1st_stage": "Configurations used in the first stage of training",
        "train_2nd_stage": "Configurations used in the second stage of training",
    }

    cs_names = ["1st_stage", "2nd_stage"]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({regex}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        cs_id = client.insert_configuration_set(
            co_ids, ds_id=ds_id, description=desc, name=cs_names[i]
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

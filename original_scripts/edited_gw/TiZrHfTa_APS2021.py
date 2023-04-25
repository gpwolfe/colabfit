#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/gubaev/TiZrHfTa_APS2021/train.cfg"
)
DATASET = "TiZrHfTa_APS2021"


LINKS = [
    "https://doi.org/10.1103/PhysRevMaterials.5.073801",
]
AUTHORS = [
    "Konstantin Gubaev",
    "Yuji Ikeda",
    "Ferenc Tasnádi",
    "Jörg Neugebauer",
    "Alexander V. Shapeev",
    "Blazej Grabowski",
    "Fritz Körmann",
]
DS_DESC = (
    "A dataset used to train machine-learning interatomic potentials (moment tensor "
    "potentials) for multicomponent alloys to ab initio data in order to investigate "
    "the disordered body-centered cubic (bcc) TiZrHfTax system with varying Ta "
    "concentration. "
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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    configurations = list(
        load_data(
            file_path=DATASET_FP,
            file_format="cfg",
            name_field=None,
            elements=["Ti", "Zr", "Hf", "Ta"],
            default_name=DATASET,
            verbose=True,
        )
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "GPa"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }

    # client.insert_property_definition('potential-energy.json')
    # client.insert_property_definition('atomic-forces.json')
    # client.insert_property_definition('cauchy-stress.json')

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

    len(set(all_co_ids))

    len(set(all_pr_ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


"""

configuration_label_regexes = {
    ".*": "active_learning",
}
for regex, labels in configuration_label_regexes.items():
    client.apply_labels(
        dataset_id=ds_id,
        collection_name='configurations',
        query={'hash': {'$in': all_co_ids}, 'names': {'$regex': regex}},
        labels=labels,
        verbose=True
    )
"""


if __name__ == "__main__":
    main(sys.argv[1:])

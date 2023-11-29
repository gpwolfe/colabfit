#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/gubaev/TiZrHfTa_APS2021/train.cfg"
)

DATASET_FP = Path().cwd().parent / "data/tizrhfta_aps2021/train.cfg"
DATASET = "TiZrHfTa_APS2021"

PUBLICATION = "https://doi.org/10.1103/PhysRevMaterials.5.073801"
DATA_LINK = None
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
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "incar": {
        "value": {"kpoints": "3 x 3 x 3", "ismear": 1, "sigma": 0.1, "encut": 250}
    },
}


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
                "_metadata": PI_MD,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": PI_MD,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "GPa"},
                "_metadata": PI_MD,
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

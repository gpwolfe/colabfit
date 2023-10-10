"""
File notes
-------------
Check names of files on kubernetes
Publications do not list software used, obtained from Dr. Gasteiger.
"""


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd

from argparse import ArgumentParser
from pathlib import Path
import sys

LINKS = [
    "https://doi.org/10.48550/arXiv.2011.14115",
    "https://doi.org/10.6084/m9.figshare.13289165.v1",
]
AUTHORS = [
    "Johannes Gasteiger",
    "Shankari Giri",
    "Johannes T. Margraf",
    "Stephan Günnemann",
]
DS_DESC = (
    "Consists of configurations taken from molecular collisions of different small "
    "organic molecules. Energies and forces for 140,000 random snapshots taken from "
    "these trajectories were recomputed with density functional theory (DFT). "
    "These calculations were performed with the revPBE functional and def2-TZVP basis, "
    "including D3 dispersion corrections"
)
DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/COLL")
# DATASET_FP = Path("data/COLL") # remove
DS_NAME = "COLL"


def tform(c):
    c.info["per-atom"] = False


atomization_property_definition = {
    "property-id": "atomization-energy",
    "property-name": "atomization-energy",
    "property-title": "Energy required to break a molecule into separate atoms",
    "property-description": "Energy required to break a molecule into separate atoms",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "The atomization energy of the molecule",
    },
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "ORCA"},
                "method": {"value": "DFT"},
                "basis-set": {"value": "def2-TZVP"},
            },
        }
    ],
    "atomization-energy": [
        {
            "energy": {"field": "atomization_energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "ORCA"},
                "method": {"value": "DFT"},
                "basis-set": {"value": "def2-TZVP"},
            },
        }
    ],
}


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
    fn_name_desc = [
        ("test", "COLL-test", "Test set from COLL. "),
        ("train", "COLL-train", "Training set from COLL. "),
        ("val", "COLL-validation", "Validation set from COLL. "),
    ]
    for fn, ds_name, desc in fn_name_desc:
        configurations = load_data(
            file_path=DATASET_FP / f"coll_v1.2_AE_{fn}.xyz",
            file_format="xyz",
            name_field=None,
            elements=["Si", "O", "C", "H"],
            default_name=ds_name,
            verbose=False,
            generator=False,
        )

        client.insert_property_definition(potential_energy_pd)

        client.insert_property_definition(atomization_property_definition)

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
            ds_id=ds_id,
            do_hashes=all_pr_ids,
            name=ds_name,
            authors=AUTHORS,
            links=LINKS,
            description=desc + DS_DESC,
            resync=True,
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

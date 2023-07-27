"""
File notes
-------------
Double check name of dataset, description of dataset, and which properties are included.
"""


from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd

from argparse import ArgumentParser
from pathlib import Path

LINKS = [
    "https://openreview.net/forum?id=HS_sOaxS9K-",
    "https://figshare.com/articles/dataset/COLL_Dataset_v1_2/13289165",
]
AUTHORS = ["Johannes Gasteiger", "Florian Becker", "Stephan Günnemann"]
DS_DESC = (
    "Consists of configurations taken from molecular collisions of different small "
    "organic molecules. Energies and forces for 140,000 random snapshots taken from "
    "these trajectories were recomputed with density functional theory (DFT). "
    "These calculations were performed with the revPBE functional and def2-TZVP basis, "
    "including D3 dispersion corrections"
)
DATASET_FP = Path("/large_data/new_raw_datasets_2.0/Coll/")
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
        "description": "enthalpy of formation",
    },
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "GPAW and VASP"},
                "method": {"value": "DFT"},
                "ecut": {"value": "700 eV for GPAW, 900 eV for VASP"},
            },
        }
    ],
    "atomization-energy": [
        {
            "energy": {"field": "atomization_energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "GPAW and VASP"},
                "method": {"value": "DFT"},
                "ecut": {"value": "700 eV for GPAW, 900 eV for VASP"},
            },
        }
    ],
    # 'atomic-forces': [{
    #     'forces':   {'field': 'forces',  'units': 'eV/Ang'},
    #         '_metadata': {
    #         'software': {'value':'VASP'},
    #     }
    # }],
    # 'cauchy-stress': [{
    # need to check unit for stress
    #     'stress':   {'field': 'virials',  'units': 'GPa'},
    #
    #     '_metadata': {
    #         'software': {'value':'GPAW and VASP'},
    #         'method':{'value':'DFT'},
    #         'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
    #     }
    # }],
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
        ("Coll_test", "COLL-test", "Test set from COLL. "),
        ("Coll_train", "COLL-train", "Train set from COLL. "),
        ("Coll_validation", "COLL-validation", "Validation set from COLL. "),
    ]
    for fn, ds_name, desc in fn_name_desc:
        configurations = load_data(
            file_path=DATASET_FP / f"{fn}.xyz",
            file_format="xyz",
            name_field="config_type",
            elements=["Si", "O", "C", "H"],
            default_name=fn,
            verbose=False,
            generator=False,
        )

        """
        cs_list = set()
        for c in configurations:
            cs_list.add(*c.info['_name'])
        print(cs_list)
        """

        client.insert_property_definition(potential_energy_pd)
        # client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
        # client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')

        client.insert_property_definition(atomization_property_definition)

        ds_id = client.generate_ds_id()

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
        ds_id = client.insert_dataset(
            ds_id=ds_id,
            do_hashes=all_pr_ids,
            name=ds_name,
            authors=AUTHORS,
            links=LINKS,
            description=desc + DS_DESC,
            resync=True,
            verbose=False,
        )

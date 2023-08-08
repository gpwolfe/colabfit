"""

File notes
----------
Fix ds description

"""

from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, free_energy_pd
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

DATASET_FP = Path("")
DS_NAME = "flexible_molecules_JCP2021"
LINKS = [
    "https://doi.org/10.1063/5.0038516",
]
AUTHORS = [
    "Valentin Vassilev-Galindo",
    "Gregory Fonseca",
    "Igor Poltavsky",
    "Alexandre Tkatchenko",
]
DS_DESC = (
    "All calculations were performed in FHI-aims software using the "
    "Perdew-Burke-Ernzerhof (PBE) exchange-correlation functional with tight settings "
    "and the Tkatchenko-Scheffler (TS) method to account for van der Waals (vdW) "
    "interactions."
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
    ds_id = generate_ds_id()

    configurations = load_data(
        file_path="/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_inversion_reformat.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "N", "H", "O"],
        default_name="Azobenzene_inversion",
        verbose=True,
        generator=False,
    )

    configurations = load_data(
        file_path="/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_rotation_and_inversion_reformat.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "N", "H", "O"],
        default_name="Azobenzene_rotation_and_inversion",
        verbose=True,
        generator=False,
    )

    configurations = load_data(
        file_path="/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_rotation_reformat.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "N", "H", "O"],
        default_name="Azobenzene_rotation",
        verbose=True,
        generator=False,
    )

    configurations = load_data(
        file_path="/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Glycine_reformat.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "N", "H", "O"],
        default_name="Glycine",
        verbose=True,
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)
    """
    free_property_definition = {
        'property-id': 'free-energy',
        'property-name': 'free-energy',
        'property-title': 'molecular reference energy',
        'property-description': 'enthalpy of formation',
        'energy': {'type': 'float', 'has-unit': True, 'extent': [], 'required': True,
                'description': 'enthalpy of formation'}}

    client.insert_property_definition(free_property_definition)
    """

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "Kcal/Mol"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "FHI-aims"},
                    "method": {"value": "DFT-PBE"},
                    # 'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
                },
            }
        ],
    }

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

    cs_info = [
        {
            "name": "Azobenzene_inversion",
            "description": "Configurations with Azobenzene inversion structure",
        },
        {
            "name": "Azobenzene_rotation_and_inversiont",
            "description": "Configurations with Azobenzene rotation and inversion "
            "structure",
        },
        {
            "name": "Azobenzene_rotation",
            "description": "Configurations with Azobenzene rotation structure",
        },
        {"name": "Glycine", "description": "Configurations with Glycine structure"},
    ]

    cs_ids = []

    for i in cs_info:
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            query={"names": {"$regex": i["name"] + "_*"}},
            name=i["name"],
            description=i["description"],
        )

        cs_ids.append(cs_id)

    ds_id = client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

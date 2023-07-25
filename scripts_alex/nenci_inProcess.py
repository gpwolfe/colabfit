from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd

from argparse import ArgumentParser
import ase
from pathlib import Path
import sys

DS_NAME = "NENCI-2021"
AUTHORS = [
    "Zachary M. Sparrow",
    "Brian G. Ernst",
    "Paul T. Joo",
    "Ka Un Lao",
    "Robert A. DiStasio, Jr",
]
LINKS = [
    "https://doi.org/10.1063/5.0068862",
]
DATASET_FP = Path("/large_data/new_raw_datasets_2.0/nenci2021/nenci2021/xyzfiles/")
DATASET_FP = Path("data/nenci_si/nenci2021/xyzfiles")  # remove
DS_DESC = (
    "NENCI-2021 is a database of approximately 8000 benchmark Non-Equilibirum "
    "Non-Covalent Interaction (NENCI) energies for a diverse selection of "
    "intermolecular complexes of biological and chemical relevance with a "
    "particular emphasis on close intermolecular contacts."
)


def reader(file_path):
    file_name = file_path.stem
    atom = ase.io.read(file_path)
    atom.info["name"] = file_name
    yield atom


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
    ds_id = client.generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "N", "O", "F", "Cl", "Br", "S", "P"],
        reader=reader,
        glob_string="*reformat.xyz",
        # default_name='nenci',
        verbose=True,
        generator=False,
    )

    client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
    # client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
    # client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')
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
    # still need to add more properties
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "CCSD(T)/CBS", "units": "kcal/mol"},
                "per-atom": {"field": "per-atom", "units": None},
                # For metadata want: software, method (DFT-XC Functional),
                # basis information, more generic parameters
                "_metadata": {
                    # 'software': {'value':'GPAW and VASP'},
                    "method": {"value": "CCSD(T)/CBS"},
                    # 'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
                },
            }
        ],
        #    'free-energy': [{
        #        'energy': {'field': 'free_energy', 'units': 'eV'},
        #        '_metadata': {
        #            'software': {'value': 'GPAW and VASP'},
        #            'method': {'value': 'DFT'},
        #            'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
        #        }
        #    }],
        #    'atomic-forces': [{
        #        'forces':   {'field': 'forces',  'units': 'eV/Ang'},
        #            '_metadata': {
        #            'software': {'value':'VASP'},
        #        }
        #    }],
        #     'cauchy-stress': [{
        # need to check unit for stress
        #         'stress':   {'field': 'virials',  'units': 'GPa'},
        #
        #         '_metadata': {
        #             'software': {'value':'GPAW and VASP'},
        #             'method':{'value':'DFT'},
        #             'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
        #         }
        #
        #     }],
        #
    }

    def tform(c):
        c.info["per-atom"] = False

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
        {"name": "Water", "description": "Configurations with water structure"},
        {"name": "MeOH", "description": "Configurations with MeOH structure"},
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

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

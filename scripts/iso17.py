"""
author:gpwolfe

Data at:
http://quantum-machine.org/datasets/iso17.tar.gz

Change DB_PATH to reflect location of iso17 folder
Change database name as appropriate

This data represents total energy, which is not yet implemented.
Therefore, only atomic forces are imported
Unzip folder before running script.
"""
from argparse import ArgumentParser
from ase.db import connect
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    # total_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path
import sys


DB_PATH = Path("data/iso17")


def reader(filepath):
    filepath = Path(filepath)
    db = connect(filepath)
    atoms = []
    for row in db.select():
        atom = row.toatoms()
        atom.info = row.data
        atom.info["name"] = "iso17"
        atom.info["energy"] = row.key_value_pairs["total_energy"]
        atoms.append(atom)

    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")
    configurations = load_data(
        file_path=DB_PATH,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="*.db",
        generator=False,
    )

    # client.insert_property_definition(total_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "FHI-aims"},
        "method": {"value": "DFT-GGA-PBE"},
    }

    property_map = {
        # This data represents total energy, not potential energy
        # Total energy is not yet implemented as a property
        # 'total-energy': [{
        #     'energy':   {'field': 'energy',  'units': 'eV'},
        #     'per-atom': {'value': False, 'units': None},
        #     '_metadata': metadata
        # }],
        "atomic-forces": [
            {
                "forces": {"field": "atomic_forces", "units": "eV/Ang"},
                "_metadata": metadata,
            }
        ]
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_ids = []
    desc = "Configurations of C7O2H10 from ISO17"
    name = "ISO17"
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": all_co_ids}},
        ravel=True,
    ).tolist()

    print(
        "Configuration set", f"({name}):".rjust(22), f"{len(co_ids)}".rjust(7)
    )

    if len(co_ids) > 0:
        cs_id = client.insert_configuration_set(
            co_ids, description=desc, name=name
        )

        cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="ISO17_anips_2017",
        authors=[
            "K.T. Schütt, P.-J. Kindermans, H.E. Sauceda, "
            "S. Chmiela, A. Tkatchenko, K.-R. Müller"
        ],
        links=[
            "http://quantum-machine.org/datasets/",
            "http://quantum-machine.org/datasets/iso17.tar.gz",
            "https://www.nature.com/articles/s41467-019-12875-2",
        ],
        description="129 molecules of composition C7O2H10 from the QM9 dataset"
        " with 5000 conformational geometries apiece. Molecular dynamics data"
        " was simulated using the Fritz-Haber Institute ab initio simulation"
        " software.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

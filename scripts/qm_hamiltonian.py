"""
author:gpwolfe

Data at:
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_water.tgz
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_ethanol_dft.tgz
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_malondialdehyde.tgz
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_uracil.tgz

The schnorb_hamiltonian_ethanol_hf file is not included, as it is run at a
different level of theory.
Uracil file has failed multiple times to download properly--
This could be a result of the size or might be a problem with the uploaded file

Unzip files before running script:
mkdir data/schnorb_hamiltonian
tar zxvf schnorb_hamiltonian*.tgz -C data/schnorb_hamiltonian

Change DB_PATH to reflect location of parent folder
Change database name as appropriate
"""
from argparse import ArgumentParser
from ase.db import connect
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path
import sys

DB_PATH = Path("data/schnorb_hamiltonian")


def reader(filepath):
    filepath = Path(filepath)
    db = connect(filepath)
    atoms = []
    for row in db.select():
        atom = row.toatoms()
        atom.info = row.data
        atom.info["name"] = filepath.stem.split("_")[-1]
        atoms.append(atom)
        if type(atom.info["energy"] == list):
            atom.info["energy"] = float(atom.info["energy"][0])
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

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    metadata = {
        "software": {"value": ["ORCA", "SchNOrb"]},
        "method": {"value": "PBE/def2-SVP"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": metadata,
            }
        ],
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
    cs_regexes = [
        [
            "QM_Hamiltonian_set",
            ".*",
            "All configurations in the Quantum Machine Molecular \
            Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_water",
            "water",
            "All water configurations from the Quantum Machine Molecular \
            Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_ethanol_hf",
            "hf",
            "All ethanol hf configurations from the Quantum Machine Molecular \
            Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_ethanol_dft",
            "dft",
            "All ethanol dft configurations from the Quantum Machine \
                Molecular Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_malondialdehyde",
            "malondialdehyde",
            "All malondialdehyde configurations from the Quantum Machine \
                Molecular Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_uracil",
            "uracil",
            "All uracil configurations from the Quantum Machine Molecular \
                Hamiltonians and Overlap Matrices set",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )
            cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="QM_hamiltonian_nature_2019",
        authors=[
            "K. T. Schütt, M. Gastegger, A. Tkatchenko, K. R. Müller, \
                R. J. Maurer"
        ],
        links=[
            "http://quantum-machine.org/datasets/",
            "https://www.nature.com/articles/s41467-019-12875-2",
        ],
        description="~100,000 configurations of water, ethanol, "
        "malondialdehyde and uracil gathered at the PBE/def2-SVP"
        " level of theory using ORCA.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
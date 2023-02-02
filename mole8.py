"""
author:gpwolfe

Data can be downloaded from:
https://rdmc.nottingham.ac.uk/handle/internal/9356

File addresses:
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p1.zip?sequence=1&isAllowed=y
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p2.zip?sequence=2&isAllowed=y
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p3.zip?sequence=3&isAllowed=y
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p4.zip?sequence=4&isAllowed=y
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p5.zip?sequence=5&isAllowed=y
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p6.zip?sequence=6&isAllowed=y
https://rdmc.nottingham.ac.uk/bitstream/handle/internal/9356/MolE8_moldata_p7.zip?sequence=7&isAllowed=y

Unzip files to new parent folder:
mkdir <project_directory>/data/mole8
unzip -q 'MolE8_moldata_*.zip' '*.out' -d <project_directory>/data/mole8

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""
from argparse import ArgumentParser
import ase
from ase.calculators.calculator import PropertyNotImplementedError
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys


DATASET_FP = Path("data/mole8")


def reader(file_path):
    file_name = file_path.stem
    atom = ase.io.read(file_path, format="gaussian-out")
    atom.info["name"] = file_name
    # ase.io.reader fails to parse a small number of .out files
    try:
        atom.info["forces"] = atom.get_forces() * (
            ase.units.Bohr / ase.units.Hartree
        )
    except PropertyNotImplementedError:
        pass
    try:
        atom.info["potential_energy"] = (
            atom.get_total_energy() / ase.units.Hartree
        )
    except PropertyNotImplementedError:
        pass
    yield atom


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "N", "O", "H"],
        reader=reader,
        glob_string="*.out",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    metadata = {
        "software": {"value": "Gaussian 09"},
        "method": {"value": ["B3LYP/6-31g(2df,p)", "DFT"]},
    }

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "Hartree/Bohr"},
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

    cs_ids = []
    description = "All molecules from the MolE8 dataset"

    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": all_co_ids}},
        ravel=True,
    ).tolist()

    cs_id = client.insert_configuration_set(
        co_ids, description=description, name="MolE8"
    )

    cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="MolE8",
        authors=["S. Lee, K. Ermanis, J.M. Goodman"],
        links=[
            "https://rdmc.nottingham.ac.uk/handle/internal/9356",
            "http://doi.org/10.17639/nott.7159",
            "https://doi.org/10.1039/D1SC06324C",
        ],
        description="About 59,000 molecular structures used for training the "
        "MolE8 machine learning models. DFT calculations performed using "
        "Gaussian 09 at the B3LYP/6-31g(2df,p) level of theory.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

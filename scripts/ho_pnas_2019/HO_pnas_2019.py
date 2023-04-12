"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2018.0020/v1

The only file necessary is:
https://archive.materialscloud.org/record/file?filename=training-set.zip&record_id=71

File address:

Unzip file to a new parent directory before running script.
mkdir <project_dir>/scripts/ho_pnas_2019
unzip training-set.zip "*.xyz"  -d <project_directory>/scripts/ho_pnas_2019/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""
from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("training-set")


def reader(file_path):
    file_name = file_path.stem
    atoms = ase.io.read(file_path, index=":")
    for atom in atoms:
        atom.info["name"] = file_name
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    # Load from colabfit's definitions
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "LAMMPS, i-PI"},
        "method": {"value": "DFT-revPBE0-D3"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "TotEnergy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/A"},
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

    # name = "HO_pnas_2019"
    # cs_ids = []
    # co_ids = client.get_data(
    #     "configurations",
    #     fields="hash",
    #     query={"hash": {"$in": all_co_ids}},
    #     ravel=True,
    # ).tolist()

    # print(
    #     "Configuration set ",
    #     f"({name}):".rjust(22),
    #     f"{len(co_ids)}".rjust(7),
    # )

    # cs_id = client.insert_configuration_set(
    #     co_ids,
    #     description="Liquid and solid H2O/water thermodynamics",
    #     name=name,
    # )

    # cs_ids.append(cs_id)
    client.insert_dataset(
        pr_hashes=all_do_ids,
        name="HO_pnas_2019",
        authors=["B. Cheng, E. Engel, J. Behler, C. Dellago, M. Ceriotti"],
        links=[
            "https://archive.materialscloud.org/record/2018.0020/v1",
            "https://www.pnas.org/doi/full/10.1073/pnas.1815117116",
        ],
        description="1590 configurations of H2O/water "
        "with total energy and forces calculated using "
        "a hybrid approach at DFT/revPBE0-D3 level of theory.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

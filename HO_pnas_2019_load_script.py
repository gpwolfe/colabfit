"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2018.0020/v1

The only file necessary is:
https://archive.materialscloud.org/record/file?filename=training-set.zip&record_id=71

File address:

Unzip file to a new parent directory before running script.
mkdir <project_dir>/data/HO_pnas_2019

tar xf training-set.zip  -C <project_directory>/data/HO_pnas_2019/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""
from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    # total energy not yet implemented in colabfit module
    # total_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path

# import property_definitions_additional as pda
import sys

DATASET_FP = Path("data/HO_pnas_2019")


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
    # client.insert_property_definition(pda.total_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "LAMMPS, i-PI"},
        "method": {"value": "DFT-revPBE0-D3"},
    }
    property_map = {
        # "total-energy": [
        #     {
        #         "energy": {"field": "TotEnergy", "units": "eV"},
        #         "per-atom": {"value": False, "units": None},
        #         "_metadata": metadata,
        #     }
        # ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/Ang"},
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
    hashes = client.get_data("configurations", fields=["hash"])
    name = "HO_pnas_2019"
    cs_ids = []
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": hashes}},
        ravel=True,
    ).tolist()

    print(
        "Configuration set ",
        f"({name}):".rjust(22),
        f"{len(co_ids)}".rjust(7),
    )

    cs_id = client.insert_configuration_set(
        co_ids,
        description="Liquid and solid H2O/water thermodynamics",
        name=name,
    )

    cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="HO_pnas_2019",
        authors=["B. Cheng, E. Engel, J. Behler, C. Dellago, M. Ceriotti"],
        links=[
            "https://archive.materialscloud.org/record/2018.0020/v1",
            "https://www.pnas.org/doi/full/10.1073/pnas.1815117116",
        ],
        description="1590 configurations of H2O/water "
        "with total energy and forces calculated using "
        "a hybrid approach, DFT and revPBE0-D3.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

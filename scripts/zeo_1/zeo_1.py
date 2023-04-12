"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2021.171

File address:
https://archive.materialscloud.org/record/file?filename=zeo-1.tar.gz&record_id=1083

Unzip files to script directory.
tar xf zeo-1.tar.gz -C $project_dir/scripts/zeo_1

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces
stress

Other properties added to metadata:
None

File notes
----------
"""
from argparse import ArgumentParser
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    cauchy_stress_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("npz")


def reader(file):
    npz = np.load(file)
    name = file.stem
    atoms = []
    for xyz, lattice, energy, stress, gradients, charges in zip(
        npz["xyz"],
        npz["lattice"],
        npz["energy"],
        npz["stress"],
        npz["gradients"],
        npz["charges"],
    ):
        atoms.append(
            Atoms(
                numbers=npz["numbers"],
                positions=xyz,
                cell=lattice,
                pbc=True,
                info={
                    "name": name,
                    "potential_energy": energy,
                    "cauchy_stress": stress,
                    "nuclear_gradients": gradients,
                    "partial_charges": charges,
                },
            )
        )
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=[
            "O",
            "Si",
            "Ge",
            "Li",
            "H",
            "Al",
            "K",
            "Ca",
            "C",
            "N",
            "Na",
            "F",
            "Ba",
            "Cs",
            "Be",
        ],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)
    metadata = {
        "software": {"value": "Amsterdam Modeling Suite"},
        "method": {"value": "revPBE"},
        "partial-charge": {"field": "partial_charges"},
        "nuclear-gradients": {"field": "nuclear_gradients"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "a.u."},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "cauchy_stress", "units": "a.u."},
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
    # co_ids = client.get_data(
    #     "configurations",
    #     fields="hash",
    #     query={"hash": {"$in": all_co_ids}},
    #     ravel=True,
    # ).tolist()

    # desc = "All configurations from Zeo-1 dataset"
    # cs_ids = []
    # cs_id = client.insert_configuration_set(
    #     co_ids, description=desc, name="Zeo-1"
    # )
    # cs_ids.append(cs_id)
    client.insert_dataset(
        pr_hashes=all_do_ids,
        name="Zeo-1_SD_2022",
        authors=["Leonid Komissarov", "Toon Verstraelen"],
        links=[
            "https://doi.org/10.24435/materialscloud:cv-zd",
            "https://doi.org/10.1038/s41597-022-01160-5",
        ],
        description="130,000 configurations of zeolite from the "
        "Database of Zeolite Structures. Calculations performed using "
        "Amsterdam Modeling Suite software.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

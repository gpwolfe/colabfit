"""
author:gpwolfe

Script assumes file has been unzipped and placed in the following relative
path (name of file unchanged after unzipping):
./data/

Data can be downloaded from:
https://doi.org/10.24435/materialscloud:14-4m
File download address:
https://archive.materialscloud.org/record/file?filename=gfn-xtb-si.tar.gz&record_id=1032

Create new directory and extract data to directory
mkdir <project_dir>/gfn_data
tar -xf gfn-xtb-si.tar.gz -C <project_dir>/gfn_data/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy

Other properties added to metadata:
nuclear gradients

File notes
----------
"""
from argparse import ArgumentParser
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("gfn_data/npz/")


def reader(file):
    npz = np.load(file)
    name = file.stem
    atoms = []
    for xyz, energy, gradients in zip(npz["xyz"], npz["energy"], npz["gradients"]):
        atoms.append(
            Atoms(
                numbers=npz["numbers"],
                positions=xyz,
                pbc=False,
                info={
                    "name": name,
                    "potential_energy": energy,
                    "nuclear_gradients": gradients,
                },
            )
        )
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
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
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["O", "Si", "C", "H", "N", "Cl", "S", "F", "P", "Br"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": "ADF"},
        "method": {"value": "revPBE"},
    }
    co_md_map = {
        "nuclear-gradients": {"field": "nuclear_gradients"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        name="GFN-xTB_jcim_2021",
        authors=["Leonid Komissarov", "Toon Verstraelen"],
        links=[
            "https://doi.org/10.24435/materialscloud:14-4m",
            "https://doi.org/10.1021/acs.jcim.1c01170",
        ],
        description="10,000 configurations of organosilicon compounds "
        "with energies predicted by an improved GFN-xTB Hamiltonian "
        "parameterization, using revPBE.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

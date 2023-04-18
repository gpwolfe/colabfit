"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.6084/m9.figshare.12672038.v3
Exact file location:
https://figshare.com/ndownloader/files/23950376

Extract files
tar xf rmd17.tar.bz2 -C <project_dir>/scripts/rmd17

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
MD-17 index

File notes
----------
"""
from argparse import ArgumentParser
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path().cwd()


def reader(file):
    atoms = []
    with np.load(file) as npz:
        npz = np.load(file)
        for coords, energy, forces, md17_index in zip(
            npz["coords"],
            npz["energies"],
            npz["forces"],
            npz["old_indices"],
        ):
            atoms.append(
                Atoms(
                    numbers=npz["nuclear_charges"],
                    positions=coords,
                    info={
                        "name": f"{file.stem}",
                        "energy": energy,
                        "forces": forces,
                        "md17_index": md17_index,
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
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "ORCA"},
        "method": {"value": "DFT-PBE"},
        "basis_set": {"value": "def2-SVP"},
        "MD17-index": {"field": "md17_index"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/mol/A"},
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
        ["azobenzene", "azobenzene", "Azobenzene rmd17 configurations"],
        ["benzene", r"^benzene", "Benzene rmd17 configurations"],
        ["aspirin", "aspirin", "Aspirin rmd17 configurations"],
        ["ethanol", "ethanol", "Ethanol rmd17 configurations"],
        [
            "malonaldehyde",
            "malonaldehyde",
            "Malonaldehyde rmd17 configurations",
        ],
        ["naphthalene", "naphthalene", "Naphthalene rmd17 configurations"],
        ["paracetamol", "paracetamol", "Paracetamol rmd17 configurations"],
        ["salicylic", "salicylic", "Salicylic rmd17 configurations"],
        ["toluene", "toluene", "Toluene rmd17 configurations"],
        ["uracil", "uracil", "Uracil rmd17 configurations"],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name="rMD17",
        authors=["Anders S. Christensen", "O. Anatole von Lilienfeld"],
        links=[
            "https://doi.org/10.6084/m9.figshare.12672038.v3",
            "https://doi.org/10.48550/arXiv.2007.09593",
        ],
        description="A dataset of 10 molecules (aspirin, "
        "azobenzene, benzene, ethanol, malonaldehyde, naphthalene, "
        "paracetamol, salicylic, toluene, uracil) with 100,000 structures"
        "calculated for each at the PBE/def2-SVP level of theory using ORCA."
        "Based on the MD17 dataset, but with refined measurements.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

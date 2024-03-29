"""
author:gpwolfe

Data can be downloaded from:

Download link:

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
virial

Other properties added to metadata
----------------------------------
energy sigma
virial sigma

File notes
----------
example info
'config_type': 'diamond111',
'energy_sigma': 0.07200000000000001,
'virial_sigma': 3.6,
'dft_energy': -13751.03625464858,
'dft_virial':
"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    cauchy_stress_pd,
    potential_energy_pd,
    atomic_forces_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data"
    "/si_h_gap/Si-H-GAP-main/structural_data"
)
DATASET_FP = Path().cwd().parent / ("data/si_h_gap")  # remove
DATASET = "Si-H-GAP"

SOFTWARE = "Quantum ESPRESSO"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1103/PhysRevMaterials.6.065603"
DATA_LINK = "https://github.com/dgunruh/Si-H-GAP"
LINKS = [
    "https://github.com/dgunruh/Si-H-GAP",
    "https://doi.org/10.1103/PhysRevMaterials.6.065603",
]
AUTHORS = [
    "Davis Unruh",
    "Reza Vatan Meidanshahi",
    "Stephen M. Goodnick",
    "Gábor Csányi",
    "Gergely T. Zimányi",
]
ELEMENTS = ["Si", "H"]


def reader(filepath):
    atoms = read(filepath, index=":", format="extxyz")
    for i, atom in enumerate(atoms):
        atom.info["name"] = f"{filepath.stem}_{atom.info['config_type']}_{i}"
        if atom.info.get("dft_virial") is not None:
            atom.info["stress"] = np.array(atom.info.get("dft_virial")).reshape(3, 3)
    return atoms


def train_reader(fp_paper):
    fp_alt = next(
        DATASET_FP.rglob("training_structures_alternate_parameterization.xyz")
    )
    atoms = read(fp_paper, index=":", format="extxyz")
    alts = read(fp_alt, index=":", format="extxyz")
    sigmas = [config.info["virial_sigma"] for config in alts]
    for i, atom in enumerate(atoms):
        atom.info["name"] = f"training_structures_{atom.info['config_type']}_{i}"
        atom.info["virial_sigma_paper"] = atom.info.pop("virial_sigma", None)
        atom.info["virial_sigma_alternate"] = sigmas[i]
        if atom.info.get("dft_virial") is not None:
            atom.info["stress"] = np.array(atom.info.get("dft_virial")).reshape(3, 3)

    return atoms


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {
                "encut": {"value": 42, "units": "rydberg"},
                "kspacing": {"value": 0.2, "units": "Ang^-1"},
            }
        },
    }
    co_md_map = {
        "energy-sigma": {"field": "energy_sigma"},
        "virial-sigma": {"field": "virial_sigma"},
        "force-atom-sigma": {"field": "force_atom_sigma"},
        "virial-sigma-paper": {"field": "virial_sigma_paper"},
        "virial-sigma-alternate": {"field": "virial_sigma_alternate"}
        # "": {"field": ""}
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "dft_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "dft_force", "units": "eV/angstrom"},
                "_metadata": metadata,
            }
        ],
    }
    glob_dss = [
        (
            "reference_structures.xyz",
            "reference",
            "A reference set of configurations of hydrogenated liquid and "
            "amorphous silicon from the datasets for Si-H-GAP. These configurations "
            "were used to evaluate training on a GAP model.",
        ),
        (
            "training_structures_paper_parameterization.xyz",
            "training",
            "A set of training configurations of hydrogenated liquid and "
            "amorphous silicon from the datasets for Si-H-GAP. Includes virial sigmas "
            "used for configurations used in the corresponding publication "
            "(virial-sigma-paper) as well as an alternate configuration defined "
            "by doubled virial sigma prefactors (from 0.025 to 0.05).",
        ),
        (
            "validation_structures.xyz",
            "validation",
            "A set of validation configurations of hydrogenated liquid and "
            "amorphous silicon from the datasets for Si-H-GAP. These configurations "
            "served to augment the reference set as a final benchmark for NEP model "
            "performance.",
        ),
    ]
    for glob_ds in glob_dss:
        if "training" in glob_ds[1]:
            configurations = load_data(
                file_path=DATASET_FP,
                file_format="folder",
                name_field="name",
                elements=ELEMENTS,
                reader=train_reader,
                glob_string=glob_ds[0],
                generator=False,
            )
        else:
            configurations = load_data(
                file_path=DATASET_FP,
                file_format="folder",
                name_field="name",
                elements=ELEMENTS,
                reader=reader,
                glob_string=glob_ds[0],
                generator=False,
            )
        ds_id = generate_ds_id()
        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                co_md_map=co_md_map,
                property_map=property_map,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        client.insert_dataset(
            ds_id=ds_id,
            # cs_ids=cs_ids,
            do_hashes=all_do_ids,
            name=f"{DATASET}_{glob_ds[1]}",
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=glob_ds[2],
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

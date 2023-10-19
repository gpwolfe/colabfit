"""
authors: Gregory Wolfe, Alexander Tao


File notes
----------


"""
from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id

# from colabfit.tools.configuration import AtomicConfiguration
from pathlib import Path
import sys
from tqdm import tqdm

from ase import Atoms
import numpy as np

from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)

# DATASET_FP = Path(
#     "/persistent/colabfit_raw_data/new_raw_datasets/Solvated_protein_UnkeOliverMeuwly"
# ) #  Kubernetes pod
DATASET_FP = Path("solvated_protein")  # Greene
# DATASET_FP = Path().cwd().parent / "data/solvated_protein"  # local
DATASET_NAME = "solvated_protein_fragments"

SOFTWARE = "ORCA 4.0.1"
METHODS = "DFT-revPBE-D3(BJ)"
BASIS = "def2-TZVP"
LINKS = [
    "https://doi.org/10.1021/acs.jctc.9b00181",
    "https://doi.org/10.5281/zenodo.2605372",
]
AUTHORS = ["Oliver T. Unke", "Markus Meuwly"]
DATASET_DESC = (
    "The solvated protein fragments dataset was generated as a "
    "benchmark for measuring the performance of machine learning models, "
    "and in particular PhysNet, for describing chemical reactions, long-range "
    "interactions, and condensed phase systems. "
    "The solvated protein fragments dataset probes many-body "
    'intermolecular interactions between "protein fragments" and water molecules, '
    "which are important for the description of many biologically relevant"
    "condensed phase systems. It contains structures for all possible"
    '"amons" (hydrogen-saturated covalently bonded fragments) of up to eight heavy '
    "atoms (C, N, O, S) that can be derived from chemical graphs of proteins "
    "containing the 20 natural amino acids connected via peptide bonds or disulfide "
    "bridges. For amino acids that can occur in different charge states due to (de)"
    "protonation (i.e. carboxylic acids that can be negatively charged or amines that "
    "can be positively charged), all possible structures with up to a total "
    "charge of +-2e "
    "are included. In total, the dataset provides reference energies, forces, "
    "and dipole "
    "moments for 2731180 structures calculated at the revPBE-D3(BJ)/def2-TZVP level of "
    "theory using the ORCA 4.0.1 code."
)
ELEMENTS = [""]
GLOB_STR = "*.npz"

PI_MD = {
    "software": {"value": SOFTWARE},
    "methods": {"value": METHODS},
    "basis-set": {"value": BASIS},
}
PROPERTY_MAP = {
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "ref_energy", "units": "eV"},
            "_metadata": PI_MD,
        }
    ],
}
CO_MD = {
    "dipole": [
        {
            "dipole_moment": {"field": "dipole_moment"},
        }
    ],
    "charge": [{"charge": {"field": "charge"}}],
}

REF_ENERGY = {
    1: -13.717939590030356,
    6: -1029.831662730747,
    7: -1485.40806126101,
    8: -2042.7920344362644,
    16: -10831.264715514206,
}


# solvated_protein-check out README in data's directory
def reader_protein(p):
    # atoms = []
    data = np.load(p)
    num_atoms = data["N"]
    atom_num = data["Z"]
    energy = data["E"]
    coords = data["R"]
    forces = data["F"]
    dipole = data["D"]
    total_charge = data["Q"]
    for i, num in tqdm(enumerate(num_atoms[:1000])):
        numbers = atom_num[i, :num]
        atom = Atoms(numbers=numbers, positions=coords[i, :num, :])
        atom.info["energy"] = float(energy[i])
        atom.info["forces"] = forces[i, :num, :]
        atom.info["dipole_moment"] = dipole[i]
        atom.info["charge"] = float(total_charge[i])
        atom.info["name"] = f"solvated_protein_{i}"
        atom.info["ref_energy"] = sum([REF_ENERGY[x] for x in numbers])
        yield atom
    # return atoms


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
    # Local
    # client = MongoDatabase(
    #     args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    # )
    # Greene
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:30007"
    )

    ds_id = generate_ds_id()
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "N", "O", "S", "H"],
        default_name="solvated_protein",
        reader=reader_protein,
        glob_string=GLOB_STR,
        verbose=True,
        generator=True,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

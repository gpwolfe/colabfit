"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
potential energy w/ reference energy
atomic forces

Other properties added to metadata
----------------------------------
dipoles
charges

File notes
----------
small enough to run from ingest pod

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.atoms import Atoms
import numpy as np
from tqdm import tqdm

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


# DATASET_FP = Path(
#     "/persistent/colabfit_raw_data/new_raw_datasets/SN2_UnkeOliverMeuwly/"
# )
DATASET_FP = Path("data/sn2_reactions.npz")
DATASET_NAME = "SN2_JCTC_2019"

SOFTWARE = "ORCA 4.0.1"
METHODS = "DSD-BLYP-D3(BJ)"
BASIS = "def2-TZVP"

PUBLICATION = "https://doi.org/10.1021/acs.jctc.9b00181"
DATA_LINK = "https://doi.org/10.5281/zenodo.2605341"
LINKS = [
    "https://doi.org/10.1021/acs.jctc.9b00181",
    "https://doi.org/10.5281/zenodo.2605341",
]
AUTHORS = ["Oliver T. Unke", "Markus Meuwly"]
DATASET_DESC = (
    "The SN2 dataset was generated as a partner benchmark dataset, "
    "along with the 'solvated protein fragments' dataset, for measuring the "
    "performance of machine learning models, in particular PhysNet, "
    "at describing chemical reactions, long-range interactions, "
    "and condensed phase systems. SN2 probes chemical reactions of "
    "methyl halides with halide anions, i.e. X- + CH3Y -> CH3X +  Y-, and "
    "contains structures, "
    "for all possible combinations of X,Y = F, Cl, Br, I. The dataset also includes "
    "various structures for several smaller molecules that can be formed in "
    "fragmentation reactions, such as CH3X, HX, CHX or CH2X- as well as geometries "
    "for H2, CH2, CH3+ and XY interhalogen compounds. In total, the dataset provides "
    "reference energies, forces, and dipole moments for 452709 structures"
    "calculated at the DSD-BLYP-D3(BJ)/def2-TZVP level of theory using ORCA 4.0.1."
)
ELEMENTS = ["C", "F", "Cl", "Br", "H", "I"]
GLOB_STR = "sn2_reactions.npz"


def reader(fp):
    # atoms = []
    data = np.load(fp)
    num_atoms = data["N"]
    atom_num = data["Z"]
    energy = data["E"]
    coords = data["R"]
    forces = data["F"]
    dipole = data["D"]
    total_charge = data["Q"]
    for i, num in tqdm(enumerate(num_atoms)):
        numbers = atom_num[i, :num]
        atom = Atoms(numbers=numbers, positions=coords[i, :num, :])
        atom.info["energy"] = float(energy[i])
        atom.info["forces"] = forces[i, :num, :]
        atom.info["dipole_moment"] = dipole[i]
        atom.info["charge"] = float(total_charge[i])
        atom.info["name"] = f"solvated_protein_{i}"
        atom.info["ref_energy"] = sum([REF_ENERGY[x] for x in atom.symbols])
        yield atom
    # return atoms


PI_MD = {
    "software": {"value": SOFTWARE},
    "methods": {"value": METHODS},
    "basis-set": {"value": BASIS},
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
    "H": -13.579407869766147,
    "C": -1028.9362774711024,
    "F": -2715.578463075019,
    "Cl": -12518.663203367176,
    "Br": -70031.09203874589,
    "I": -8096.587166328217,
}


def tform(c):
    c.info["per-atom"] = False


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
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    ds_id = generate_ds_id()
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=ELEMENTS,
        default_name="SN2",
        reader=reader,
        glob_string=GLOB_STR,
        verbose=True,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations,
            property_map=PROPERTY_MAP,
            generator=False,
            ds_id=ds_id,
            transform=tform,
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

#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
import h5py
import json
from pathlib import Path
import sys

from ase import Atoms
import numpy as np
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/QM9x")

DATASET_FP = Path().cwd().parent / "data/qm9x"
DATASET = "QM9x"
AUTHORS = [
    "Mathias Schreiner",
    "Arghya Bhowmik",
    "Tejs Vegge",
    "Jonas Busk",
    "Ole Winther",
]

PUBLICATION = "https://doi.org/10.1038/s41597-022-01870-w"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.20449701.v2"
LINKS = [
    "https://doi.org/10.1038/s41597-022-01870-w",
    "https://doi.org/10.6084/m9.figshare.20449701.v2",
]
DS_DESC = (
    "Dataset containing DFT calculations of energy and forces for all configurations "
    "in the QM9 dataset, recalculated with the ωB97X functional and 6-31G(d) basis "
    "set. Recalculating the energy and forces causes a slight shift of the potential "
    "energy surface, which results in forces acting on most configurations in the "
    "dataset. The data was generated by running Nudged Elastic "
    "Band (NEB) calculations with DFT on 10k reactions while saving intermediate "
    "calculations. QM9x is used as a benchmarking and comparison dataset for the "
    "dataset Transition1x."
)

REFERENCE_ENERGIES = {
    1: -13.62222753701504,
    6: -1029.4130839658328,
    7: -1484.8710358098756,
    8: -2041.8396277138045,
    9: -2712.8213146878606,
}
PI_MD = {
    "software": {"value": "ORCA 5.0.2"},
    "method": {"value": "DFT-ωB97X"},
    "basis": {"value": "6-31G(d)"},
}
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "atomization-energy": [
        {
            "energy": {"field": "atomization_energy", "units": "eV"},
            "_metadata": PI_MD,
        }
    ],
}


def tform(c):
    c.info["per-atom"] = False


def get_molecular_reference_energy(atomic_numbers):
    molecular_reference_energy = 0
    for atomic_number in atomic_numbers:
        molecular_reference_energy += REFERENCE_ENERGIES[atomic_number]

    return molecular_reference_energy


def generator(formula, grp):
    """Iterates through a h5 group"""

    energies = grp["energy"]
    forces = grp["forces"]
    atomic_numbers = list(grp["atomic_numbers"])
    positions = grp["positions"]
    molecular_reference_energy = get_molecular_reference_energy(atomic_numbers)

    for energy, force, positions in zip(energies, forces, positions):
        d = {
            "energy": energy.__float__(),
            "atomization_energy": energy - molecular_reference_energy.__float__(),
            "forces": force.tolist(),
            "positions": positions,
            "formula": formula,
            "atomic_numbers": atomic_numbers,
        }

        yield d


class Dataloader:
    """
    Can iterate through h5 dataset

    hdf5_file: path to data
    only_final: if True, the iterator will only loop through reactant, product and
    transition state instead of all configurations for each reaction and return
    them in dictionaries.
    """

    def __init__(self, h5file):
        self.h5file = h5file

    def __iter__(self):
        with h5py.File(self.h5file, "r") as f:
            for formula, grp in f.items():
                for mol in generator(formula, grp):
                    yield mol


def reader(path):
    adl = Dataloader(path)
    images = []
    for data in adl:
        # for i in range(data['positions'].shape[0]):
        atoms = Atoms(symbols=data["atomic_numbers"], positions=data["positions"])

        # atoms.info["name"] = "{}_{}_conformer_{}".format(
        #     os.path.split(path)[-1], data["path"], i
        # )

        atoms.info["per-atom"] = False

        atoms.info["energy"] = data["energy"]
        atoms.info["atomization_energy"] = data["atomization_energy"]
        atoms.info["formula"] = data["formula"]
        atoms.arrays["forces"] = np.array(data["forces"])
        # atoms.info['hirdipole'] = data['hirdipole'][i]
        # atoms.info['hirshfeld'] = data['hirshfeld'][i]
        # atoms.info['spin-densities'] = data['spindensities'][i]

        images.append(atoms)

    return images


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

    with open("atomization_energy.json", "r") as f:
        atomization_energy_pd = json.load(f)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(atomization_energy_pd)
    configurations = list(
        load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="formula",
            elements=["C", "H", "O", "N", "F"],
            default_name="QM9x",
            verbose=False,
            reader=reader,
            glob_string="*.h5",
        )
    )
    cs_list = set()
    for c in configurations:
        cs_list.add(*c.info["_name"])
    """
    atomization_property_definition = {
        "property-id": "atomization-energy",
        "property-name": "atomization-energy",
        "property-title": "energy minus molecular reference energy",
        "property-description": "the difference between energy and molecular "
        "reference energy",
        "energy": {
            "type": "float",
            "has-unit": True,
            "extent": [],
            "required": True,
            "description": "the difference between energy and molecular "
            "reference energy",
        },
    }
    """
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=False,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

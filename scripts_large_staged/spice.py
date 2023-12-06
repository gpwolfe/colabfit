"""
author: Gregory Wolfe

Properties
----------
potential energy
formation energy

Other properties added to metadata
----------------------------------

File notes
----------
These keys appear in all configs:

Read once:
[('atomic_numbers', (27,)),
('smiles', (1,)),
('subset', (1,)),

Iterate over zip:
('conformations', (50, 27, 3)),
('dft_total_energy', (50,)),
('dft_total_gradient', (50, 27, 3)),
('formation_energy', (50,)),
('mayer_indices', (50, 27, 27)),
('scf_dipole', (50, 3)),
('scf_quadrupole', (50, 3, 3)),
('wiberg_lowdin_indices', (50, 27, 27))
('wiberg_lowdin_indices', (50, 27, 27))]

These appear in most but not all:
Iterate over zip:
('mbis_charges', (50, 27, 1)),
('mbis_dipoles', (50, 27, 3)),
('mbis_octupoles', (50, 27, 3, 3, 3)),
('mbis_quadrupoles', (50, 27, 3, 3)),
"""
from argparse import ArgumentParser
import functools
import h5py
import json
import logging
from pathlib import Path
import sys
import time

from ase.atoms import Atoms
from tqdm import tqdm
import pymongo
from pymongo.errors import InvalidOperation

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    potential_energy_pd,
)


DATASET_FP = Path("data/spice")
DATASET_NAME = "SPICE_2023"
LICENSE = "Creative Commons 0"

SOFTWARE = "Psi4 1.4.1"
METHODS = "DFT-ωB97M-D3(BJ)"
BASIS_SET = "def2-TZVPPD"

PUBLICATION = "https://doi.org/10.1038/s41597-022-01882-6"
DATA_LINK = "https://doi.org/10.5281/zenodo.8222043"
OTHER_LINKS = ["https://github.com/openmm/spice-dataset"]
LINKS = [
    "https://doi.org/10.1038/s41597-022-01882-6",
    "https://doi.org/10.5281/zenodo.8222043",
    "https://github.com/openmm/spice-dataset",
]
AUTHORS = [
    "Peter Eastman",
    "Pavan Kumar Behara",
    "David L. Dotson",
    "Raimondas Galvelis",
    "John E. Herr",
    "Josh T. Horton",
    "Yuezhi Mao",
    "John D. Chodera",
    "Benjamin P. Pritchard",
    "Yuanqing Wang",
    "Gianni De Fabritiis",
    "Thomas E. Markland",
]
DATASET_DESC = (
    "SPICE (Small-Molecule/Protein Interaction Chemical Energies) is a "
    "collection of quantum mechanical data for training potential functions. "
    "The emphasis is particularly on simulating drug-like small molecules "
    "interacting with proteins. Subsets of the dataset include the following: "
    "dipeptides: these provide comprehensive sampling of the covalent interactions "
    "found in proteins; solvated amino acids: these provide sampling of "
    "protein-water and water-water interactions; PubChem molecules: These sample a "
    "very wide variety of drug-like small molecules; monomer and dimer structures "
    "from DES370K: these provide sampling of a wide variety of non-covalent "
    "interactions; ion pairs: these provide further sampling of Coulomb "
    "interactions over a range of distances."
)
ELEMENTS = [
    "H",
    "Li",
    "C",
    "N",
    "O",
    "F",
    "Na",
    "Mg",
    "P",
    "S",
    "Cl",
    "K",
    "Ca",
    "Br",
    "I",
]
GLOB_STR = "*.hdf5"

# Assign additional relevant property instance metadata, such as basis set used
PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "basis-set": {"value": BASIS_SET},
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "Hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "formation-energy": [
        {
            "energy": {"field": "form_energy", "units": "Hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_KEYS = [
    # 'atomic_numbers',
    #  'conformations',
    #  'dft_total_energy',
    #  'formation_energy',
    "dft_total_gradient",
    "mayer_indices",
    "mbis_charges",  # Not all
    "mbis_dipoles",  # Not all
    "mbis_octupoles",  # Not all
    "mbis_quadrupoles",  # Not all
    "scf_dipole",
    "scf_quadrupole",
    "smiles",
    "subset",
    "wiberg_lowdin_indices",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


def h5_generator(grp, key):
    """Iterates through an h5 group"""

    energies = grp["dft_total_energy"]
    formation_energies = grp["formation_energy"]
    # forces = grp["wB97x_6-31G(d).forces"]
    positions = grp["conformations"]

    other_dict = dict()
    other_dict["dft_total_gradient"] = grp["dft_total_gradient"]
    other_dict["mayer_indices"] = grp.get("mayer_indices")
    other_dict["mbis_charges"] = grp.get("mbis_charges")
    other_dict["mbis_dipoles"] = grp.get("mbis_dipoles")
    other_dict["mbis_octupoles"] = grp.get("mbis_octupoles")
    other_dict["mbis_quadrupoles"] = grp.get("mbis_quadrupoles")
    other_dict["scf_dipole"] = grp["scf_dipole"]
    other_dict["scf_quadrupole"] = grp["scf_quadrupole"]
    other_dict["wiberg_lowdin_indices"] = grp["wiberg_lowdin_indices"]

    smiles = grp["smiles"][0]
    atomic_numbers = list(grp["atomic_numbers"])
    subset = grp["subset"][0].decode("utf-8")

    for i, row in enumerate(zip(energies, formation_energies, positions)):
        config = Atoms(positions=row[2], numbers=atomic_numbers)
        # config.info["forces"] = force.tolist()
        config.info["energy"] = float(row[0])
        config.info["form_energy"] = float(row[1])
        config.info["smiles"] = smiles
        config.info["name"] = f"{key}_{subset}"
        config.info.update(
            {key: values[i] for key, values in other_dict.items() if values is not None}
        )
        yield config


def reader(fp):
    with h5py.File(fp, "r") as f:
        for i, key in tqdm(enumerate(list(f.keys())[9::10])):
            configs = h5_generator(f[key], key)
            for config in configs:
                yield config


MAX_AUTO_RECONNECT_ATTEMPTS = 100


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


@auto_reconnect
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
    with open("formation_energy.json") as f:
        formation_energy_pd = json.load(f)
    client.insert_property_definition(formation_energy_pd)
    client.insert_property_definition(potential_energy_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # If no obvious divisions between configurations exist (i.e., different methods or
    # materials), remove the following lines through 'cs_ids.append(...)' and from
    # 'insert_dataset(...) function remove 'cs_ids=cs_ids' argument.

    cs_names = (
        "SPICE DES Monomers Single Points Dataset v1.1",
        "SPICE DES370K Single Points Dataset Supplement v1.0",
        "SPICE DES370K Single Points Dataset v1.0",
        "SPICE Dipeptides Single Points Dataset v1.2",
        "SPICE Ion Pairs Single Points Dataset v1.1",
        "SPICE PubChem Set 1 Single Points Dataset v1.2",
        "SPICE PubChem Set 2 Single Points Dataset v1.2",
        "SPICE PubChem Set 3 Single Points Dataset v1.2",
        "SPICE PubChem Set 4 Single Points Dataset v1.2",
        "SPICE PubChem Set 5 Single Points Dataset v1.2",
        "SPICE PubChem Set 6 Single Points Dataset v1.2",
        "SPICE Solvated Amino Acids Single Points Dataset v1.1",
    )

    cs_ids = []

    for name in cs_names:
        try:
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=name,
                description=(
                    "Configurations in the SPICE dataset from "
                    f"{name.replace('SPICE ', '')}"
                ),
                query={"names": {"$regex": name}},
            )

            cs_ids.append(cs_id)
        except InvalidOperation:
            pass

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

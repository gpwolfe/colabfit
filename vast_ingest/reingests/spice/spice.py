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

import os
from pathlib import Path
from time import time

import h5py
from ase.atoms import Atoms
from dotenv import load_dotenv
from tqdm import tqdm

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    SparkDataLoader,
)
from colabfit.tools.property_definitions import (
    formation_energy_pd,
    energy_pd,
)

# Set up data loader environment
load_dotenv()
loader = SparkDataLoader(
    table_prefix="ndb.colabfit.dev",
)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

# Define which tables will be used


# loader.config_table = "ndb.colabfit.dev.co_spice"
# loader.prop_object_table = "ndb.colabfit.dev.po_spice"
# loader.config_set_table = "ndb.colabfit.dev.cs_spice"
# loader.dataset_table = "ndb.colabfit.dev.ds_spice"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip2"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip2"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/spice/SPICE-1.1.4.hdf5"  # noqa
)
DATASET_NAME = "SPICE_2023"
LICENSE = "Creative Commons 0"

SOFTWARE = "Psi4 1.4.1"
METHODS = "DFT-ωB97M-D3(BJ)"
BASIS_SET = "def2-TZVPPD"

PUBLICATION_YEAR = "2023"
DOI = "10.60732/a613a175"
DATASET_ID = "DS_kg0dv12aiq97_0"
LICENSE = "MIT"

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
DESCRIPTION = (
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
    "keys": {
        "value": {
            energy_pd["property-name"]: "dft_total_energy",
            formation_energy_pd["property-name"]: "formation_energy",
        }
    },
}

PI_ADD_KEYS = [
    "dft_total_gradient",
    "scf_dipole",
    "scf_quadrupole",
    "mbis_charges",  # Not all
    "mbis_dipoles",  # Not all
    "mbis_octupoles",  # Not all
    "mbis_quadrupoles",  # Not all
]
PI_METADATA.update({key: {"field": key} for key in PI_ADD_KEYS})


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    formation_energy_pd["property-name"]: [
        {
            "energy": {"field": "form_energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}
CO_KEYS = [
    "mayer_indices",
    "smiles",
    "subset",
    "wiberg_lowdin_indices",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


def h5_generator(grp, key):
    """Iterates through an h5 group"""

    energies = grp["dft_total_energy"]
    formation_energies = grp["formation_energy"]
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
        config.info["energy"] = float(row[0])
        config.info["form_energy"] = float(row[1])
        config.info["smiles"] = smiles
        config.info["_name"] = (
            f"{key.replace(' ', '_')}__{subset.replace(' ', '_')}__index_{i}"
        )
        config.info.update(
            {key: values[i] for key, values in other_dict.items() if values is not None}
        )
        yield AtomicConfiguration.from_ase(config, co_md_map=CO_METADATA)


def wrapper(fp):
    with h5py.File(fp, "r") as f:
        for i, key in tqdm(enumerate(list(f.keys())[9::10])):
            for config in h5_generator(f[key], key):
                yield config


cs_names = [
    x.replace(" ", "_")
    for x in [
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
    ]
]
cs_descriptions = [
    f"Configurations in the SPICE dataset from {name.replace('SPICE ', '').replace('_', ' ')}"  # noqa
    for name in cs_names
]
cs_queries = cs_names


def main():
    beg = time()
    config_generator = wrapper(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, formation_energy_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=10000,
    )
    print(f"Time to prep: {time() - beg}")
    t = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=False)
    print(f"Time to load: {time() - t}")

    print("creating config sets")
    t = time()
    css = list(zip(cs_queries, [None for x in cs_queries], cs_names, cs_descriptions))
    dm.create_configuration_sets(loader, css)
    print("Creating dataset")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
        doi=DOI,
        # labels=LABELS,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

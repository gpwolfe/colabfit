"""
nabla2DFT_Test_Conformations_Tiny ingest script for ColabFit VAST database.

Properties
----------
atomization-energy: per-conformation energy in Hartree referenced to isolated
    atoms (total DFT energy minus isolated-atom reference energies; nablaDFT
    labels this "formation energy"); auto-converted to eV on ingest.
atomic-forces: forces in Hartree/Angstrom; auto-converted to eV/Angstrom.

File notes
----------
Source: vast_ingest/nabladft/data/
        test_2k_conformers_v2_formation_energy_w_forces.db
ASE SQLite database (2774 conformations) holding the tiny ("2k") conformation
test split of nabla2DFT (the v2 expansion of nablaDFT): held-out RDKit
conformations of drug-like molecules drawn from the training set. Molecular
systems (pbc=False), so no stress is stored. Each row carries key_value_pairs
(smiles, moses_id, conformation_id) and a row.data dict with energy
(single-element list, Hartree) and forces
(natoms x 3, Hartree/Angstrom). atoms.calc is None, so energy/forces are read
from row.data rather than a calculator. The database metadata declares
units energy=Hartree, forces=Hartree/Ang, distance=Ang, and an atomrefs table,
confirming the stored energy is a formation (reference-subtracted) energy.

ASE db fields
-------------
key_value_pairs: smiles, moses_id, conformation_id
row.data: energy (Hartree), forces (Hartree/Angstrom)
Per-atom: species, positions, forces
"""

import os
from pathlib import Path

import ase.db
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    atomization_energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyInfo, PropertyMap
from dotenv import load_dotenv

load_dotenv()

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")

loader = VastDataLoader(
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
loader.config_table = "ndb.colabfit.dev.co_nabla2dft_conf_tiny"
loader.config_set_table = "ndb.colabfit.dev.cs_nabla2dft_conf_tiny"
loader.dataset_table = "ndb.colabfit.dev.ds_nabla2dft_conf_tiny"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_nabla2dft_conf_tiny"

DATASET_ID = "DS_acloh0k4dh9p_0"
DATASET_NAME = "nabla2DFT_Test_Conformations_Tiny"

DESCRIPTION = (
    "nabla2DFT (∇²DFT) is the second version of the nablaDFT dataset, a "
    "large-scale quantum-chemistry dataset of drug-like molecules; this record "
    "is its tiny (2k) conformation test split: a held-out set of RDKit "
    "conformations of molecules that also appear in the training set, provided "
    "with per-conformation formation energies (total DFT energy minus "
    "isolated-atom reference energies) and atomic forces. The full nabla2DFT "
    "dataset is a benchmark for neural network potentials, conformational "
    "energy, and electronic-structure (Hamiltonian) prediction in the "
    "multi-molecule setting; it expands the original nablaDFT release with "
    "roughly twice as many molecular structures and three times as many "
    "conformations, along with new data types, tasks, and state-of-the-art "
    "baseline models. Like nablaDFT, it is derived from a subset of the "
    "Molecular Sets (MOSES) collection (containing C, N, S, O, F, Cl, Br, and "
    "H), with conformations generated using RDKit and clustered with the Butina "
    "algorithm. For every conformation, electronic properties - including the "
    "total energy, atomic forces, and the DFT Hamiltonian and overlap matrices "
    "- were computed with Kohn-Sham DFT at the ωB97X-D/def2-SVP level using the "
    "Psi4 quantum-chemistry package (version 1.5) with a Lebedev-Treutler "
    "integration grid (Treutler atomic-weight partition, 75 radial and 302 "
    "spherical points), an SCF energy and density convergence threshold of "
    "1e-6, and an integral screening threshold of 1e-12."
)

PUBLICATION = "https://doi.org/10.48550/arXiv.2406.14347"
DATA_LINK = "https://github.com/AIRI-Institute/nablaDFT"
OTHER_LINKS = ["https://doi.org/10.1039/d2cp03966d"]
PUBLICATION_YEAR = "2024"

AUTHORS = [
    "Kuzma Khrabrov",
    "Anton Ber",
    "Artem Tsypin",
    "Konstantin Ushenin",
    "Egor Rumiantsev",
    "Alexander Telepov",
    "Dmitry Protasov",
    "Ilya Shenbin",
    "Anton Alekseev",
    "Mikhail Shirokikh",
    "Sergey Nikolenko",
    "Elena Tutubalina",
    "Artur Kadurin",
]
# nabla2DFT data are distributed under the MIT license of the AIRI-Institute
# repository; the NeurIPS 2024 / arXiv article itself carries a separate license.
LICENSE = "MIT"
DOI = None

DATA_PATH = (
    Path(__file__).parent
    / "data"
    / "test_2k_conformers_v2_formation_energy_w_forces.db"
)


def reader(db_path: Path):
    """Yield AtomicConfiguration objects from the nablaDFT ASE SQLite db."""
    db = ase.db.connect(str(db_path))
    for row in db.select():
        atoms = row.toatoms()
        moses_id = row.get("moses_id")
        conf_id = row.get("conformation_id")
        smiles = row.get("smiles")
        energy = row.data["energy"]
        if isinstance(energy, (list, tuple)):
            energy = energy[0]
        forces = row.data["forces"]
        atoms.info = {
            "_name": f"{DATASET_NAME}__moses_{moses_id}__conf_{conf_id}",
            "energy": energy,
            "forces": forces,
            "smiles": smiles,
            "moses_id": moses_id,
            "conformation_id": conf_id,
        }
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([atomization_energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "Psi4 1.5")
property_map.set_metadata_field("method", "DFT-ωB97X-D")
property_map.set_metadata_field(
    "input",
    {
        "functional": "ωB97X-D",
        "basis_set": "def2-SVP",
        "integration_grid": (
            "Lebedev-Treutler (Treutler partition), 75 radial / 302 "
            "spherical points"
        ),
        "scf_convergence": "1e-6 (energy and density)",
        "integral_threshold": "1e-12",
        "energy_type": (
            "formation energy (total DFT energy minus isolated-atom "
            "reference energies)"
        ),
        "description": (
            "Kohn-Sham DFT single-point calculations at ωB97X-D/def2-SVP "
            "with Psi4 1.5 for RDKit-generated conformations of drug-like "
            "molecules from a subset of the MOSES dataset."
        ),
    },
)
property_map.set_metadata_field("smiles", "smiles", dynamic=True)
property_map.set_metadata_field("moses_id", "moses_id", dynamic=True)
property_map.set_metadata_field("conformation_id", "conformation_id", dynamic=True)

atomization_info = PropertyInfo(
    property_name="atomization-energy",
    field="energy",
    units="hartree",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = PropertyInfo(
    property_name="atomic-forces",
    field="forces",
    units="hartree/angstrom",
    original_file_key="forces",
)

property_map.set_properties([atomization_info, force_info])
PROPERTY_MAP = property_map.get_property_map()

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[atomization_energy_pd, atomic_forces_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

dm.create_dataset(
    loader=loader,
    name=DATASET_NAME,
    authors=AUTHORS,
    publication_link=PUBLICATION,
    data_link=DATA_LINK,
    other_links=OTHER_LINKS,
    description=DESCRIPTION,
    data_license=LICENSE,
    publication_year=PUBLICATION_YEAR,
    equilibrium=False,
    date_requested="2026-06-05",
)

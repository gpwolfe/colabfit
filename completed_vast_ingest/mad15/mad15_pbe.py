"""
MAD-1.5_PBE ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures only)
atomization-energy: atomization energy in eV

File notes
----------
Source: mad15/mad-1.5-pbe.xyz

A subset of the MAD-1.5 structures recomputed with the PBE functional, covering
the MAD-1 subsets (MC3D, MC3D-rattled, MC3D-random, MC3D-surface, MC3D-cluster,
MC2D, SHIFTML-molcrys, SHIFTML-molfrags) plus monomers and MC3D-random-extended
from MAD-1.5. All other DFT settings are consistent with the r2SCAN calculations.
Cross-validation splits are consistent with the r2SCAN dataset. This combined
file contains the training, validation, and test splits.

XYZ header keys
---------------
energy, atomization_energy, stress (periodic only), subset, frame_id, pbc
Per-atom: species, pos, forces
"""

import os
from pathlib import Path

from ase.io import iread
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    atomization_energy_pd,
    cauchy_stress_pd,
    energy_pd,
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
loader.config_table = "ndb.colabfit.dev.co_mad15_pbe"
loader.config_set_table = "ndb.colabfit.dev.cs_mad15_pbe"
loader.dataset_table = "ndb.colabfit.dev.ds_mad15_pbe"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_mad15_pbe"

DATASET_ID = "DS_zs8bsrqnmyn0_0"
DATASET_NAME = "Massive_Atomic_Diversity_MAD-1.5_PBE"
DESCRIPTION = (
    "A subset of the MAD-1.5 (Massive Atomic Diversity version 1.5) structures "
    "recomputed with the PBE GGA functional, covering the MAD-1 subsets (MC3D, "
    "MC3D-rattled, MC3D-random, MC3D-surface, MC3D-cluster, MC2D, SHIFTML-molcrys, "
    "SHIFTML-molfrags) plus monomers and MC3D-random-extended from the new MAD-1.5 "
    "subsets. All DFT settings are consistent with the r2SCAN calculations: "
    "FHI-aims (version 250806) all-electron code with tight NAO basis sets "
    "(species defaults 2020), 8 Angstrom^-1 k-point density for periodic systems, "
    "Gaussian smearing of 0.05 eV, and SCF convergence thresholds of 1e-6 eV "
    "(energy), 1e-4 eV/Angstrom (forces), and 1e-5 e*a0^-3 (electron density). "
    "Cross-validation splits are consistent with the r2SCAN train/val/test splits; "
    "this file contains all three splits combined. PBE targets were used in "
    "PET-MAD-1.5 model training with separate prediction heads alongside r2SCAN "
    "targets, improving force accuracy by approximately 25% relative to r2SCAN-only "
    "training. As a lower level of theory, this dataset is less carefully curated "
    "than the primary r2SCAN dataset; PBE heads are discarded from the final "
    "released models."
)
PUBLICATION = "https://doi.org/10.48550/arXiv.2603.02089"
DATA_LINK = "https://doi.org/10.24435/materialscloud:jc-9f"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"
AUTHORS = [
    "Cesare Malosso",
    "Filippo Bigi",
    "Paolo Pegolo",
    "Joseph W. Abbott",
    "Philip Loche",
    "Mariana Rossi",
    "Michele Ceriotti",
    "Arslan Mazitov",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "mad-1.5-pbe.xyz"

SUBSET_DESCRIPTIONS = {
    "mc3d": (
        "Bulk crystals from the Materials Cloud 3D (MC3D) database, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "mc3d_rattled": (
        "Rattled configurations derived from MC3D bulk crystals, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "mc3d_random": (
        "Configurations with atomic species randomized over 85 elements in MC3D "
        "structures, part of the MAD-1.5_PBE dataset."
    ),
    "mc3d_surface": (
        "Surface slabs generated from MC3D crystals, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "mc3d_cluster": (
        "Nanoclusters cut from MC3D and MC3D-rattled structures, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "mc2d": (
        "Two-dimensional crystals from the Materials Cloud 2D (MC2D) database, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "shiftml_molcrys": (
        "Curated molecular crystals from the SHIFTML dataset, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "shiftml_molfrags": (
        "Neutral molecular fragments from the SHIFTML dataset, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "monomers": (
        "Isolated single-atom configurations covering all 102 elements, "
        "part of the MAD-1.5_PBE dataset."
    ),
    "mc3d_random_ext": (
        "Configurations with atomic species randomized over 102 elements in MC3D "
        "structures, targeting under-represented heavy elements, "
        "part of the MAD-1.5_PBE dataset."
    ),
}

CSS = [
    (f"__{subset}__", None, f"{DATASET_NAME}__{subset}", desc, False)
    for subset, desc in SUBSET_DESCRIPTIONS.items()
]


def reader(fp: Path):
    for atoms in iread(fp, format="extxyz"):
        subset = atoms.info.get("subset", "unknown")
        frame_id = atoms.info.get("frame_id", "")
        atoms.info["_name"] = f"{DATASET_NAME}__{subset}__{frame_id}"
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd, atomization_energy_pd])
property_map.set_metadata_field("software", "FHI-aims v250806")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "functional": "PBE GGA (non-spin-polarized)",
        "basis_sets": "FHI-aims tight NAO, species defaults 2020",
        "kpoint_density": "8 Angstrom^-1 (periodic systems)",
        "smearing": "Gaussian, 0.05 eV",
        "scf_energy_convergence": "1e-6 eV",
        "scf_force_convergence": "1e-4 eV/Angstrom",
        "scf_density_convergence": "1e-5 e*a0^-3",
        "notes": (
            "All settings consistent with r2SCAN MAD-1.5 calculations. "
            "For lanthanides and actinides (Pr-Yb, Pu-No, excluding Ce), "
            "the confined 5d/6d function was removed to improve SCF convergence."
        ),
    },
)
property_map.set_metadata_field("subset", "subset", dynamic=True)
property_map.set_metadata_field("frame_id", "frame_id", dynamic=True)

energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = PropertyInfo(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
)
stress_info = PropertyInfo(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
atomization_energy_info = PropertyInfo(
    property_name="atomization-energy",
    field="atomization_energy",
    units="eV",
    original_file_key="atomization_energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info, atomization_energy_info])
PROPERTY_MAP = property_map.get_property_map()

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd, atomization_energy_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
    suppress_warnings={"cauchy-stress"},
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

dm.create_configuration_sets(loader, CSS)

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
    date_requested="2026-05-21",
)

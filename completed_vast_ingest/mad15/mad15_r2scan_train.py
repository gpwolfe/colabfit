"""
MAD-1.5_r2SCAN_Train ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures only)
atomization-energy: atomization energy in eV

File notes
----------
Source: mad15/mad-1.5-r2scan-train.xyz

MAD-1.5 (Massive Atomic Diversity v1.5) is a highly curated dataset for training
broadly applicable atomistic ML models across 102 elements. All structures are
computed with a single standardized all-electron DFT workflow using r2SCAN in
FHI-aims (v250806) with tight basis sets, 8 Angstrom^-1 k-point density, and Gaussian
smearing of 0.05 eV. The training split (~83%) includes all monomers, dimers,
and trimers to anchor low-body-order interactions.

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
loader.config_table = "ndb.colabfit.dev.co_mad15_r2sc_trn"
loader.config_set_table = "ndb.colabfit.dev.cs_mad15_r2sc_trn"
loader.dataset_table = "ndb.colabfit.dev.ds_mad15_r2sc_trn"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_mad15_r2sc_trn"

DATASET_ID = "DS_okaut8m5c2vk_0"
DATASET_NAME = "Massive_Atomic_Diversity_MAD-1.5_r2SCAN_Train"
DESCRIPTION = (
    "Training split of the MAD-1.5 (Massive Atomic Diversity version 1.5) dataset, "
    "a highly curated collection designed for training broadly applicable atomistic "
    "machine-learning models across the full periodic table. MAD-1.5 extends the "
    "original MAD dataset with targeted enrichment strategies covering 102 chemical "
    "elements (all isotopes with half-life above one day). All 216,803 structures are "
    "computed with a single standardized all-electron DFT workflow using the r2SCAN "
    "meta-GGA functional in FHI-aims (version 250806), with tight basis sets, 8 Angstrom^-1 "
    "k-point density, Gaussian smearing of 0.05 eV, and SCF convergence thresholds "
    "of 1e-6 eV (energy), 1e-4 eV/Angstrom (forces), and 1e-5 e*a0^-3 (electron density). "
    "The dataset spans molecules (monomers, dimers, trimers, molecular crystals), "
    "bulk crystals, surfaces, nanoclusters, and low-dimensional structures organized "
    "into 14 subsets. Quality is ensured by two-step outlier removal: heuristic "
    "filtering of structures with forces >100 eV/Angstrom, followed by LLPR "
    "uncertainty-based filtering. The training split (~83% of cleaned data) includes "
    "all monomers, dimers, and trimers to anchor low-body-order interactions. A "
    "companion PBE-functional dataset (Massive_Atomic_Diversity_MAD-1.5_PBE) was used during model training "
    "with separate prediction heads."
)
PUBLICATION = "https://doi.org/10.48550/arXiv.2603.02089"
DATA_LINK = "https://doi.org/10.24435/materialscloud:jc-9f"
OTHER_LINKS = ["https://github.com/lab-cosmo/upet"]
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

DATA_PATH = Path(__file__).parent / "mad-1.5-r2scan-train.xyz"

SUBSET_DESCRIPTIONS = {
    "mc3d": (
        "Bulk crystals from the Materials Cloud 3D (MC3D) database, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc3d_rattled": (
        "Rattled configurations derived from MC3D bulk crystals, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc3d_random": (
        "Configurations with atomic species randomized over 85 elements in MC3D "
        "structures, part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc3d_surface": (
        "Surface slabs generated from MC3D crystals, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc3d_cluster": (
        "Nanoclusters cut from MC3D and MC3D-rattled structures, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc2d": (
        "Two-dimensional crystals from the Materials Cloud 2D (MC2D) database, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "shiftml_molcrys": (
        "Curated molecular crystals from the SHIFTML dataset, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "shiftml_molfrags": (
        "Neutral molecular fragments from the SHIFTML dataset, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "monomers": (
        "Isolated single-atom configurations covering all 102 elements, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "dimers": (
        "Isolated two-atom configurations sampling all unique element pairs from "
        "102 elements at 10 interatomic distances, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "trimers": (
        "Isolated three-atom configurations with randomly selected elements from "
        "102 elements, part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc3d_ext": (
        "Extended MC3D crystals covering 93 elements with Z<100, including "
        "lanthanides and actinides, part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "mc3d_random_ext": (
        "Configurations with atomic species randomized over 102 elements in MC3D "
        "structures, targeting under-represented heavy elements, "
        "part of the MAD-1.5_r2SCAN_Train dataset."
    ),
    "binary_random": (
        "Binary FCC and BCC random substitutional decorations over all pairs of "
        "102 elements, part of the MAD-1.5_r2SCAN_Train dataset."
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
property_map.set_metadata_field("method", "DFT-r2SCAN")
property_map.set_metadata_field(
    "input",
    {
        "functional": "r2SCAN meta-GGA (non-spin-polarized)",
        "basis_sets": "FHI-aims tight NAO, species defaults 2020",
        "kpoint_density": "8 Angstrom^-1 (periodic systems)",
        "smearing": "Gaussian, 0.05 eV",
        "scf_energy_convergence": "1e-6 eV",
        "scf_force_convergence": "1e-4 eV/Angstrom",
        "scf_density_convergence": "1e-5 e*a0^-3",
        "notes": (
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

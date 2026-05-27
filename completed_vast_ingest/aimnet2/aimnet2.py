"""
AIMNet2 ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy without dispersion contribution, eV
atomic-forces: atomic forces without dispersion contribution, eV/Angstrom

File notes
----------
Source: vast_ingest/aimnet2/aimnet2_2025_b973c.h5

3,765,011 molecular configurations spanning 1-193 atoms per system,
covering 14 non-metal elements (H, B, C, N, O, F, Si, P, S, Cl, As,
Se, Br, I) in neutral and charged closed-shell states. This is the
extended training dataset used for continual pretraining of AIMNet2(2025)
to improve noncovalent interaction (NCI) descriptions relative to the
original AIMNet2(2023) model (~20M structures).

HDF5 structure: groups named by atom count (001-193), each with:
  coord:   (N, n_atoms, 3) float32  Cartesian coordinates, Angstrom
  numbers: (N, n_atoms)   int8/16   atomic numbers
  charge:  (N,)           int8/16   molecular charge, e
  energy:  (N,)           float64   total energy w/o dispersion, eV
  forces:  (N, n_atoms, 3) float64  atomic forces w/o dispersion, eV/Ang
  charges: (N, n_atoms)   float64   Hirshfeld partial charges (not ingested)
  dipole:  (N, 3)         float64   molecular dipole moment (not ingested)
"""

import os
from pathlib import Path

import h5py
import numpy as np
from ase import Atoms as AseAtoms
from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd
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
loader.config_table = "ndb.colabfit.dev.co_aimnet2"
loader.config_set_table = "ndb.colabfit.dev.cs_aimnet2"
loader.dataset_table = "ndb.colabfit.dev.ds_aimnet2"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_aimnet2"

DATASET_ID = "DS_skz6qt7fghs8_0"
DATASET_NAME = "AIMNet2"
DESCRIPTION = (
    "AIMNet2(2025) is the extended training dataset for the AIMNet2 "
    "(second generation atoms-in-molecules network) neural network "
    "interatomic potential, curated to improve the model's description "
    "of noncovalent interactions (NCIs) including hydrogen bonding, "
    "pi-pi stacking, dispersion, sigma-hole, ionic, and electrostatic "
    "contacts. The dataset covers neutral and charged closed-shell "
    "molecular systems composed of up to 14 non-metal elements "
    "(H, B, C, N, O, F, Si, P, S, Cl, As, Se, Br, I) with up to 193 "
    "atoms per system. Structures "
    "were drawn from three complementary sources: (a) molecular "
    "geometries from SPICE v2.0.1 (solvated systems, amino acid-ligand "
    "pairs, water clusters) and the CREMP dataset (macrocyclic peptides); "
    "(b) small neutral and charged molecules from PubChem sampled via "
    "normal mode sampling and metadynamics-guided geometry exploration; "
    "(c) dimer geometries assembled from Cambridge Structural Database "
    "(CSD) monomers (up to 14 supported elements, fewer than 200 atoms) "
    "and pre-optimized with AIMNet2-wB97M-D3(2023) to remove steric "
    "clashes while preserving configurational diversity. All quantum "
    "chemical calculations used ORCA 6.0.1 with the composite B97-3c "
    "DFT functional under restricted Kohn-Sham (RKS) formalism. SCF "
    "convergence was enforced with TightSCF and SlowConv; RIJCOSX "
    "integral acceleration and DEFGRID2 integration grid were applied "
    "throughout. AIMNet2(2025) was initialized from AIMNet2(2023) "
    "weights and continually pretrained on this dataset without weight "
    "freezing or regularization, using a multi-task loss over energy "
    "(w=1.0), forces (w=0.2), and Hirshfeld partial charges (w=0.5)."
)

PUBLICATION = "https://doi.org/10.26434/chemrxiv.15000203/v1"
DATA_LINK = "https://doi.org/10.1184/R1/31141138"
OTHER_LINKS = ["https://github.com/isayevlab/aimnetcentral"]
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Kamal Singh Nayal",
    "Ilkwon Cho",
    "Runtian Nick Gao",
    "Peikun Zheng",
    "Olexandr Isayev",
]
LICENSE = "MIT"
DOI = None

DATA_PATH = Path(__file__).parent / "aimnet2_2025_b973c.h5"


def reader(h5_path: Path):
    with h5py.File(h5_path, "r") as f:
        for group_name in sorted(f.keys()):
            grp = f[group_name]
            coords = grp["coord"][:]  # (N, n_atoms, 3) float32, Ang
            numbers = grp["numbers"][:]  # (N, n_atoms) int8/int16
            energies = grp["energy"][:]  # (N,) float64, eV
            forces = grp["forces"][:]  # (N, n_atoms, 3) float64, eV/Ang
            n_confs = energies.shape[0]
            for i in range(n_confs):
                atoms = AseAtoms(
                    numbers=numbers[i].astype(np.int32),
                    positions=coords[i],
                    pbc=False,
                )
                atoms.info = {
                    "_name": f"{DATASET_NAME}__{group_name}__{i:07d}",
                    "energy": float(energies[i]),
                    "forces": forces[i].tolist(),
                }
                yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "ORCA 6.0.1")
property_map.set_metadata_field("method", "DFT-B97-3c")
property_map.set_metadata_field(
    "input",
    {
        "basis_set": "def2-mTZVP (composite, part of B97-3c)",
        "dispersion_correction": "D3BJ (composite, part of B97-3c)",
        "counterpoise_correction": "gCP (composite, part of B97-3c)",
        "scf_convergence": "TightSCF with SCFConvForced and SlowConv",
        "integral_acceleration": "RIJCOSX (ORCA default)",
        "integration_grid": "DEFGRID2 (ORCA default DFT grid)",
        "scf_algorithm": ("NoTRAH initially; TRAH scheme enabled if SCF did not converge"),
        "formalism": "Restricted Kohn-Sham (RKS), closed-shell only",
        "orca_input_template": (
            "! b97-3c tightscf engrad SCFConvForced slowconv notrah | "
            "%elprop dipole true end | "
            "%output Print[P_Hirshfeld] 1 end"
        ),
        "description": (
            "Energy and forces are reported without "
            "the dispersion contribution. Neutral and charged systems "
            "used an identical SCF setup for methodological consistency. "
        ),
    },
)

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
property_map.set_properties([energy_info, force_info])
PROPERTY_MAP = property_map.get_property_map()

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd],
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
    date_requested="2026-05-26",
)

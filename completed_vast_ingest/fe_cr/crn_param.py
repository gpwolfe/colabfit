"""
CrN-mMTP-Param ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Å
cauchy-stress: PlusStress/volume in eV/Å³ as 3x3 matrix

File notes
----------
Source: datasets_for_magnetic_MTP-main/CrN_paramagnetic_AL/training_set.cfg
2423 configurations of 64-atom B1-CrN supercells with collinear atomic
magnetic moments and magnetic forces. Created via active learning. Data in
MLIP-2 .cfg format with en_der_mx/y/z columns (magnetic forces in eV/μ_B).
PlusStress = stress × volume (eV). DFT computed with ABINIT using constrained
DFT (cDFT) and PAW PBE. The fitted mMTP reproduces elastic constants, phonon
spectrum, linear thermal expansion coefficient, and specific heat capacity of
paramagnetic B1-CrN in agreement with DFT and experiment.
"""

import os
from pathlib import Path

import numpy as np
from ase import Atoms as AseAtoms
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyInfo, PropertyMap
from dotenv import load_dotenv
from vastdb.session import Session

load_dotenv()

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

loader = VastDataLoader(
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
loader.config_table = "ndb.colabfit.dev.co_crn_param"
loader.config_set_table = "ndb.colabfit.dev.cs_crn_param"
loader.dataset_table = "ndb.colabfit.dev.ds_crn_param"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_crn_param"

DATASET_ID = "DS_zjs2zamw2hvn_0"
DATASET_NAME = "CrN-mMTP-Paramagnetic-ActiveLearning"
DESCRIPTION = (
    "Training set for a magnetic Moment Tensor Potential (mMTP) for "
    "paramagnetic B1-CrN, created via active learning. Contains 2423 "
    "configurations of 64-atom CrN supercells with collinear atomic "
    "magnetic moments and magnetic forces (negative derivatives of energy "
    "with respect to magnetic moments, in eV/mu_B). Configurations generated "
    "using constrained DFT (cDFT) with ABINIT and PAW PBE pseudopotentials "
    "with a 6x6x6 k-point mesh and 25 Hartree plane-wave cutoff energy. The "
    "fitted mMTP accurately reproduces elastic constants, phonon spectrum, "
    "linear thermal expansion coefficient, and specific heat capacity of "
    "paramagnetic B1-CrN, with thermal properties (quasi-harmonic "
    "approximation) in good agreement with experimental results. Note: "
    "ColabFit dataset contains energy, atomic forces, and stress. "
    "Refer to the original files for per-atom magnetic moment and magnetic "
    "force data."
)

PUBLICATION = "https://doi.org/10.48550/arXiv.2412.20214"
DATA_LINK = "https://gitlab.com/ivannovikov/datasets_for_magnetic_MTP"
OTHER_LINKS = None
PUBLICATION_YEAR = "2024"

AUTHORS = [
    "Alexey S. Kotykhov",
    "Max Hodapp",
    "Christian Tantardini",
    "Konstantin Kravtsov",
    "Ivan Kruglov",
    "Alexander V. Shapeev",
    "Ivan S. Novikov",
]
LICENSE = "CC-BY-4.0"
DOI = None

SPECIES_MAP = {0: "N", 1: "Cr"}
DATA_PATH = (
    Path().parent
    / "datasets_for_magnetic_MTP-main"
    / "CrN_paramagnetic_AL"
    / "training_set.cfg"
)


def stress_ev_a3(plus_stress, cell):
    """Convert PlusStress (stress×volume in eV) to eV/Å³ 3x3 symmetric matrix.

    PlusStress component order: xx yy zz yz xz xy
    """
    volume = abs(np.linalg.det(np.array(cell)))
    xx, yy, zz, yz, xz, xy = [s / volume for s in plus_stress]
    return [[xx, xy, xz], [xy, yy, yz], [xz, yz, zz]]


def parse_cfg_block(block):
    """Parse a single MLIP-2 .cfg block (content after a BEGIN_CFG marker).

    Returns (cell, symbols, positions, forces, energy, plus_stress).
    """
    cell, positions, symbols, forces = [], [], [], []
    energy, plus_stress, n_atoms = None, None, 0
    lines = block.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith("END_CFG") or line.startswith("Feature"):
            i += 1
            continue
        if line == "Size":
            i += 1
            n_atoms = int(lines[i].strip())
        elif line == "Supercell":
            for _ in range(3):
                i += 1
                cell.append([float(v) for v in lines[i].strip().split()])
        elif line.startswith("AtomData:"):
            cols = line.split()[1:]
            for _ in range(n_atoms):
                i += 1
                d = dict(zip(cols, lines[i].strip().split()))
                symbols.append(SPECIES_MAP[int(d["type"])])
                positions.append(
                    [
                        float(d["cartes_x"]),
                        float(d["cartes_y"]),
                        float(d["cartes_z"]),
                    ]
                )
                forces.append([float(d["fx"]), float(d["fy"]), float(d["fz"])])
        elif line == "Energy":
            i += 1
            energy = float(lines[i].strip())
        elif line.startswith("PlusStress:"):
            i += 1
            plus_stress = [float(v) for v in lines[i].strip().split()]
        i += 1
    return cell, symbols, positions, forces, energy, plus_stress


def reader(filepath):
    with open(filepath, "r") as f:
        content = f.read()
    for idx, block in enumerate(content.split("BEGIN_CFG")[1:]):
        cell, symbols, positions, forces, energy, ps = parse_cfg_block(block)
        stress = stress_ev_a3(ps, cell)
        atoms = AseAtoms(symbols=symbols, positions=positions, cell=cell, pbc=True)
        atoms.info = {
            "_name": f"{DATASET_NAME}__{idx}",
            "energy": energy,
            "forces": forces,
            "stress": stress,
        }
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "ABINIT")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "description": (
            "Constrained DFT (cDFT) with PAW PBE pseudopotentials. "
            "6x6x6 k-point mesh. Plane-wave cutoff: 25 Hartree (~680 eV). "
            "Collinear magnetic moments fixed via cDFT constraints."
        ),
    },
)

energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="Energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = PropertyInfo(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="fx,fy,fz",
)
stress_info = PropertyInfo(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="PlusStress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info])
PROPERTY_MAP = property_map.get_property_map()

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
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
    date_requested="2026-04-13",
)

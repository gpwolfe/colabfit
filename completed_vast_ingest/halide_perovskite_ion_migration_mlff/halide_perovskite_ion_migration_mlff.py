"""
Halide_Perovskite_Ion_Migration_MLFF ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in kbar (auto-converted to eV/Angstrom^3)

File notes
----------
Source: vast_ingest/halide_perovskite_ion_migration_mlff/
        halide_perovskite_ion_migration_mlff/ML_AB_*
Seven VASP ML_AB machine-learning-force-field training files for CsPbI3 halide
perovskite, used to trace iodide vacancy and interstitial migration (in
neutral and charged states) and the cubic-tetragonal phase transition. Each
ML_AB configuration block stores the cell, Cartesian positions, total energy
(eV), forces (eV/Angstrom), and stress (kbar). Reference data were computed
with VASP using the PBE functional with Grimme D3 dispersion and
Becke-Johnson damping (PBE-D3-BJ) on 2x2x2 cubic supercells (~40 atoms).

Each ML_AB file is ingested as its own standalone dataset (seven sister
datasets with distinct dataset IDs), since each training set targets a
different characteristic of the perovskite.

ML_AB block layout
------------------
"Atom types and atom numbers": per-type symbol and count (atom ordering)
"Primitive lattice vectors (ang.)": 3x3 cell
"Atomic positions (ang.)": Cartesian coordinates
"Total energy (eV)": scalar
"Forces (eV ang.^-1)": per-atom forces
"Stress (kbar)": Voigt order XX YY ZZ then XY YZ ZX
"""

import os
from pathlib import Path

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

load_dotenv()

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")

loader = VastDataLoader(
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
loader.config_table = "ndb.colabfit.dev.co_cspbi3_mlff"
loader.dataset_table = "ndb.colabfit.dev.ds_cspbi3_mlff"

BASE_NAME = "Halide_Perovskite_Ion_Migration_MLFF"

PUBLICATION = "https://doi.org/10.1021/acs.jpclett.5c01139"
DATA_LINK = "https://doi.org/10.5281/zenodo.19951919"
OTHER_LINKS = ["https://arxiv.org/abs/2409.16051"]
PUBLICATION_YEAR = "2025"

AUTHORS = [
    "Viren Tyagi",
    "Mike Pols",
    "Geert Brocks",
    "Shuxia Tao",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "halide_perovskite_ion_migration_mlff"

# (dataset_id, name_suffix, ML_AB filename suffix, focus phrase for description)
DATASETS = [
    (
        "DS_3qkh1wmrfjz8_0",
        "phase_transition",
        "phase_transition",
        "the cubic-tetragonal phase transition",
    ),
    (
        "DS_11usai5qggq2_0",
        "neutral_iodide_interstitial",
        "neutral_iodide_intersitital",
        "migration of a neutral iodide interstitial",
    ),
    (
        "DS_7q1jmfpf3q9g_0",
        "neutral_iodide_vacancy",
        "neutral_iodide_vacancy",
        "migration of a neutral iodide vacancy",
    ),
    (
        "DS_oef7piahyylc_0",
        "positive_iodide_interstitial",
        "positively_charged_iodide_interstitial",
        "migration of a positively charged iodide interstitial",
    ),
    (
        "DS_5tjgk8acmeqe_0",
        "positive_iodide_vacancy",
        "positively_charged_iodide_vacancy",
        "migration of a positively charged iodide vacancy",
    ),
    (
        "DS_jofg74jvsu0t_0",
        "negative_iodide_interstitial",
        "negatively_charged_iodide_interstitial",
        "migration of a negatively charged iodide interstitial",
    ),
    (
        "DS_7iys5iy3bz4z_0",
        "negative_iodide_vacancy",
        "negatively_charged_iodide_vacancy",
        "migration of a negatively charged iodide vacancy",
    ),
]


def build_description(focus: str) -> str:
    return (
        "VASP machine-learning-force-field (ML_AB) training set for the "
        f"CsPbI3 halide perovskite, covering {focus}. This is one of seven "
        "sister datasets from the same publication, each providing a VASP "
        "ML_AB training set for a slightly different characteristic of the "
        "CsPbI3 perovskite (the cubic-tetragonal phase transition and the "
        "migration of iodide vacancies and interstitials in neutral, "
        "positively charged, and negatively charged states), all generated "
        "to train machine-learned force fields for ion migration. Each "
        "configuration carries the total energy, atomic forces, and stress "
        "from VASP single-point reference calculations using the PBE "
        "functional with Grimme D3 dispersion and Becke-Johnson damping "
        "(PBE-D3-BJ), on 2x2x2 cubic supercells (~40 atoms). (Plane-wave "
        "cutoff and k-point details are reported only in the paper's "
        "Supporting Information.)"
    )


def _find(lines, label, start=0):
    for i in range(start, len(lines)):
        if lines[i].strip().startswith(label):
            return i
    raise ValueError(f"Label not found: {label}")


def _floats(line):
    return [float(x) for x in line.split()]


def _parse_block(lines):
    """Parse one ML_AB configuration block into an ase Atoms object."""
    i = _find(lines, "The number of atoms")
    natoms = int(lines[i + 2])

    i = _find(lines, "Atom types and atom numbers")
    symbols = []
    j = i + 2
    while not lines[j].strip().startswith(("=", "*")):
        parts = lines[j].split()
        symbols.extend([parts[0]] * int(parts[1]))
        j += 1

    i = _find(lines, "Primitive lattice vectors")
    cell = [_floats(lines[i + 2 + k]) for k in range(3)]

    i = _find(lines, "Atomic positions")
    positions = [_floats(lines[i + 2 + k]) for k in range(natoms)]

    i = _find(lines, "Total energy")
    energy = float(lines[i + 2])

    i = _find(lines, "Forces")
    forces = [_floats(lines[i + 2 + k]) for k in range(natoms)]

    i = _find(lines, "Stress (kbar)")
    i_diag = _find(lines, "XX YY ZZ", i)
    xx, yy, zz = _floats(lines[i_diag + 2])
    i_off = _find(lines, "XY YZ ZX", i_diag)
    xy, yz, zx = _floats(lines[i_off + 2])
    stress = [[xx, xy, zx], [xy, yy, yz], [zx, yz, zz]]

    atoms = AseAtoms(symbols=symbols, positions=positions, cell=cell, pbc=True)
    atoms.info["energy"] = energy
    atoms.info["forces"] = forces
    atoms.info["stress"] = stress
    return atoms


def reader(ml_ab: Path, dataset_name: str):
    """Yield configurations from a single ML_AB file."""
    with open(ml_ab) as f:
        lines = f.readlines()
    starts = [
        i for i, ln in enumerate(lines)
        if ln.strip().startswith("Configuration num.")
    ]
    starts.append(len(lines))
    for n in range(len(starts) - 1):
        block = lines[starts[n]:starts[n + 1]]
        atoms = _parse_block(block)
        atoms.info["_name"] = f"{dataset_name}__{n}"
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-PBE+D3(BJ)")
property_map.set_metadata_field(
    "input",
    {
        "functional": "PBE with Grimme D3 dispersion, Becke-Johnson damping",
        "supercell": "2x2x2 cubic CsPbI3 (~40 atoms)",
        "mlff": "VASP on-the-fly machine-learned force field (GAP-style)",
        "description": (
            "VASP PBE-D3-BJ single-point reference data stored in ML_AB "
            "training files. Plane-wave cutoff and k-point sampling are given "
            "only in the publication's Supporting Information."
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
stress_info = PropertyInfo(
    property_name="cauchy-stress",
    field="stress",
    units="kbar",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)

property_map.set_properties([energy_info, force_info, stress_info])
PROPERTY_MAP = property_map.get_property_map()


for dataset_id, name_suffix, file_suffix, focus in DATASETS:
    dataset_name = f"{BASE_NAME}_{name_suffix}"
    ml_ab = DATA_PATH / f"ML_AB_{file_suffix}"

    gen = reader(ml_ab, dataset_name)

    dm = DataManager(
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        configs=gen,
        prop_map=PROPERTY_MAP,
        dataset_id=dataset_id,
        standardize_energy=True,
        read_write_batch_size=10000,
    )

    dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

    dm.create_dataset(
        loader=loader,
        name=dataset_name,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        other_links=OTHER_LINKS,
        description=build_description(focus),
        data_license=LICENSE,
        publication_year=PUBLICATION_YEAR,
        equilibrium=False,
        date_requested="2026-06-02",
    )

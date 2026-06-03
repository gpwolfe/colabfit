"""
CeO2_Surface_Oxygen_Vacancy_MLFF ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in kbar (auto-converted to eV/Angstrom^3)

File notes
----------
Source: vast_ingest/ceo2_surface_oxygen_vacancy_mlff/
        ceo2_surface_oxygen_vacancy_mlff/01_dataset_and_force_field/ML_ABN_*
Six VASP on-the-fly machine-learning training files (ML_AB format) holding the
DFT reference configurations for a study constructing a machine-learning force
field of CeO2 surfaces with an oxygen vacancy, via a dataset-merging strategy.
Each ML_ABN configuration block stores the cell, Cartesian positions, total
energy (eV), forces (eV/Angstrom), and stress (kbar). The merged file
(ML_ABN_merge_*) duplicates configurations already present in the individual
per-surface files and is therefore skipped. The trained ML_FFN force-field
binaries, vacancy-free/validation POSCAR structures (02/03), and NEB path
files (04) are NOT ingested.

ML_AB block layout
------------------
"The number of atoms": scalar
"Atom types and atom numbers": per-type symbol and count
"Primitive lattice vectors (ang.)": 3x3 cell
"Atomic positions (ang.)": Cartesian coordinates
"Total energy (eV)": scalar
"Forces (eV ang.^-1)": per-atom forces
"Stress (kbar)": XX YY ZZ then XY YZ ZX
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
loader.config_table = "ndb.colabfit.dev.co_ceo2_ovac_mlff"
loader.config_set_table = "ndb.colabfit.dev.cs_ceo2_ovac_mlff"
loader.dataset_table = "ndb.colabfit.dev.ds_ceo2_ovac_mlff"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_ceo2_ovac_mlff"

DATASET_ID = "DS_puukty1cgl4q_0"
DATASET_NAME = "CeO2_Surface_Oxygen_Vacancy_MLFF"
DESCRIPTION = (
    "Density functional theory reference data for constructing a "
    "machine-learning force field (MLFF) of cerium oxide (CeO2) surfaces "
    "containing an oxygen vacancy, generated with VASP on-the-fly "
    "machine-learning and stored in ML_AB training files. The dataset "
    "follows a dataset-merging strategy, combining six independently sampled "
    "surface families that vary the surface orientation (CeO2(100) "
    "Ce-terminated and CeO2(111)), slab thickness (two- vs three-layer), and "
    "oxygen-vacancy content (zero or one vacancy), for roughly 1,700 "
    "configurations carrying total energies, atomic forces, and stresses. "
    "Configuration sets group the data by surface family. Reference "
    "calculations used VASP with the spin-polarized PBE functional plus "
    "Grimme D3 dispersion and a Hubbard U correction on the Ce 4f states "
    "(DFT+U, Ueff=5.0 eV), a 520 eV plane-wave cutoff, and a 1x1x1 "
    "Gamma-centered k-point grid."
)

PUBLICATION = "https://doi.org/10.26434/chemrxiv.15003015/v1"
DATA_LINK = "https://doi.org/10.5281/zenodo.20033455"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Kai Oshiro",
    "Min Gao",
    "Jun-ya Hasegawa",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = (
    Path(__file__).parent
    / "ceo2_surface_oxygen_vacancy_mlff"
    / "01_dataset_and_force_field"
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


def reader(data_path: Path):
    for ml_ab in sorted(data_path.glob("ML_ABN_*")):
        # The merged file duplicates configs from the individual files.
        if "merge" in ml_ab.name:
            continue
        subset = ml_ab.name.replace("ML_ABN_", "")
        with open(ml_ab) as f:
            lines = f.readlines()
        starts = [
            i
            for i, ln in enumerate(lines)
            if ln.strip().startswith("Configuration num.")
        ]
        starts.append(len(lines))
        for n in range(len(starts) - 1):
            block = lines[starts[n] : starts[n + 1]]
            atoms = _parse_block(block)
            atoms.info["_name"] = f"{DATASET_NAME}__{subset}__{n}"
            atoms.info["subset"] = subset
            yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP 6.4.2")
property_map.set_metadata_field("method", "DFT-PBE+U+D3")
property_map.set_metadata_field(
    "input",
    {
        "functional": "PBE (GGA), spin-polarized",
        "dispersion_correction": "Grimme D3",
        "Hubbard_U": "DFT+U on Ce 4f states, effective U = 5.0 eV",
        "ENCUT": "520 eV",
        "kpoints": "1x1x1 Gamma-centered",
        "mlff": "VASP on-the-fly machine-learned force field (ML_AB)",
        "description": (
            "DFT reference data (PBE+U+D3, Ueff=5.0 eV on Ce 4f, ENCUT=520 eV, "
            "1x1x1 Gamma-centered k-points, spin-polarized) stored in VASP "
            "ML_AB training files."
        ),
    },
)
property_map.set_metadata_field("subset", "subset", dynamic=True)

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

SUBSETS = {
    "100-Ce_2L0V": ("CeO2(100) Ce-terminated, two-layer slab, no oxygen vacancy."),
    "100-Ce_2L1V": ("CeO2(100) Ce-terminated, two-layer slab, one oxygen vacancy."),
    "100-Ce_3L0V": ("CeO2(100) Ce-terminated, three-layer slab, no oxygen vacancy."),
    "100-Ce_3L1V": ("CeO2(100) Ce-terminated, three-layer slab, one oxygen vacancy."),
    "111_2L0V": "CeO2(111) surface, two-layer slab, no oxygen vacancy.",
    "111_3L0V": "CeO2(111) surface, three-layer slab, no oxygen vacancy.",
}

CSS = [
    (
        f"{DATASET_NAME}__{subset}__.*",
        None,
        f"{DATASET_NAME}_{subset}",
        desc,
        False,
    )
    for subset, desc in SUBSETS.items()
]

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

dm.create_configuration_sets(loader=loader, name_label_match=CSS)

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
    date_requested="2026-06-02",
)

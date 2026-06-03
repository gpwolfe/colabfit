"""
Perovskite_Nanorod_Li_Na_Transport ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV (energy(sigma->0) from OUTCAR)
atomic-forces: forces in eV/Angstrom (TOTAL-FORCE block from OUTCAR)

File notes
----------
Source: vast_ingest/perovskite_nanorod_li_na_transport/
        perovskite_nanorod_li_na_transport/**/{CONTCAR, OUTCAR}
VASP single-point (SCF) calculations supporting a study of A-site doping in
lithium-lanthanum-titanate (LMTO/LLTO) perovskite nanorods and their
interfaces with a p(MTFSI) polymer electrolyte, examining interfacial Li-ion
and Na-ion transport for composite polymer electrolytes. The structure is read
from CONTCAR and the energy/forces are parsed from the paired OUTCAR (ASE's
OUTCAR reader fails on these files due to a POTCAR/species mismatch).
Calculations used VASP with the r2SCAN meta-GGA functional (+rVV10 for
surfaces) via pymatgen's MPScanRelaxSet, PBE PAW (v54) potentials,
EDIFF=1e-5 eV, and Gaussian smearing (ISMEAR=0).
"""

import os
from pathlib import Path

from ase.io import read
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
loader.config_table = "ndb.colabfit.dev.co_lmto_pmtfsi"
loader.config_set_table = "ndb.colabfit.dev.cs_lmto_pmtfsi"
loader.dataset_table = "ndb.colabfit.dev.ds_lmto_pmtfsi"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_lmto_pmtfsi"

DATASET_ID = "DS_2t6wvm4wkqrg_0"
DATASET_NAME = "Perovskite_Nanorod_Li_Na_Transport"
DESCRIPTION = (
    "VASP single-point (SCF) DFT calculations underpinning a mechanistic study "
    "of A-site doping in lithium-lanthanum-titanate (LMTO/LLTO) perovskite "
    "nanorods and their interfaces with a p(MTFSI) polymer electrolyte, aimed "
    "at understanding interfacial Li-ion and Na-ion transport for composite "
    "polymer electrolyte design. The dataset includes bulk/reference systems "
    "(Li, Na, MTFSI, LiMTFSI, NaMTFSI, MTFSI dimers) and LMTO/polymer "
    "interface slabs at varying A-site compositions. Calculations used VASP "
    "with the r2SCAN meta-GGA functional (with rVV10 nonlocal correlation for "
    "surfaces) via pymatgen's MPScanRelaxSet, PBE PAW (version 54) potentials, "
    "EDIFF=1e-5 eV, and Gaussian smearing (ISMEAR=0). Energies and forces are "
    "taken from the SCF OUTCAR; structures from the paired CONTCAR. "
    "Configuration sets group calculations by reference/interface category."
)

PUBLICATION = "https://doi.org/10.1039/D5TA10111E"
DATA_LINK = "https://doi.org/10.5281/zenodo.18260745"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Lauren B. Shepard",
    "Ji-young Ock",
    "Amit Bhattacharya",
    "Tao Wang",
    "Albina Borisevich",
    "Michelle Lehmann",
    "Sheng Dai",
    "Raphaële Clément",
    "Alexei P. Sokolov",
    "X. Chelsea Chen",
    "Susan B. Sinnott",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "perovskite_nanorod_li_na_transport"


def _outcar_energy(outcar_path: Path):
    """Final energy(sigma->0) in eV from a VASP OUTCAR."""
    energy = None
    with open(outcar_path) as f:
        for line in f:
            if "energy(sigma->0)" in line:
                tail = line.split("energy(sigma->0)")[1]
                energy = float(tail.split("=")[1].split()[0])
    return energy


def _outcar_forces(outcar_path: Path, natoms: int):
    """Final TOTAL-FORCE block (eV/Angstrom) from a VASP OUTCAR."""
    with open(outcar_path) as f:
        lines = f.readlines()
    idx = [i for i, ln in enumerate(lines) if "TOTAL-FORCE" in ln]
    if not idx:
        return None
    start = idx[-1] + 2
    forces = []
    for k in range(natoms):
        cols = lines[start + k].split()
        forces.append([float(cols[3]), float(cols[4]), float(cols[5])])
    return forces


def reader(data_path: Path):
    for outcar in sorted(data_path.rglob("OUTCAR")):
        contcar = outcar.parent / "CONTCAR"
        if not contcar.exists():
            continue
        try:
            atoms = read(contcar, format="vasp")
        except Exception as e:
            print(f"Error reading {contcar}: {e}")
            continue
        energy = _outcar_energy(outcar)
        forces = _outcar_forces(outcar, len(atoms))
        if energy is None or forces is None:
            print(f"Missing energy/forces in {outcar}; skipping")
            continue
        rel = outcar.parent.relative_to(data_path)
        category = rel.parts[0]
        tag = str(rel).replace("/", "__")
        atoms.info["_name"] = f"{DATASET_NAME}__{category}__{tag}"
        atoms.info["category"] = category
        atoms.info["energy"] = energy
        atoms.info["forces"] = forces
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-R2SCAN")
property_map.set_metadata_field(
    "input",
    {
        "functional": "r2SCAN meta-GGA (+rVV10 nonlocal vdW for surfaces)",
        "pseudopotentials": "PBE PAW, version 54",
        "input_set": "pymatgen MPScanRelaxSet",
        "EDIFF": "1e-5 eV",
        "EDIFFG": "-0.02 eV/Angstrom",
        "ISMEAR": "0 (Gaussian)",
        "description": (
            "VASP r2SCAN (with rVV10 for surface/interface systems) via "
            "pymatgen MPScanRelaxSet; energies and forces from the SCF OUTCAR."
        ),
    },
)
property_map.set_metadata_field("category", "category", dynamic=True)

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

CATEGORIES = {
    "lithium_ref": "Bulk lithium reference calculation.",
    "sodium_ref": "Bulk sodium reference calculation.",
    "LiMTFSI_ref_calc": "MTFSI and LiMTFSI molecular reference calculations.",
    "NaMTFSI_ref_calc": "MTFSI and NaMTFSI molecular reference calculations.",
    "MTFSIdimer_ref_calc": "LiSIC/NaSIC p(MTFSI) dimer reference calculations.",
    "LMTO_LiMTFSI_calcs": "LMTO slab / LiMTFSI / MTFSI interface calculations.",
    "LMTO_NaMTFSI_calcs": "LMTO slab / NaMTFSI / MTFSI interface calculations.",
    "LMTO_MTFSIdimer_calcs": "LMTO/p(MTFSI) dimer interface calculations.",
}

CSS = [
    (
        f"{DATASET_NAME}__{cat}__.*",
        None,
        f"{DATASET_NAME}_{cat}",
        desc,
        False,
    )
    for cat, desc in CATEGORIES.items()
]

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

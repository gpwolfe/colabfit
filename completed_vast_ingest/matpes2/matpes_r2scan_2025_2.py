"""
MatPES-R2SCAN-2025.2 ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Å
cauchy-stress: stress tensor in kBar (Voigt 6-vector), converted to eV/Å³

File notes
----------
Source: MatPES-R2SCAN-2025.2-charges.json
386,544 structures sampled via the DIRECT method from 300 K NpT MD simulations
seeded from Materials Project entries. Static DFT calculations performed using
VASP with the r2SCAN meta-GGA functional and MatPESStaticSet convergence settings.

v2025.2 updates relative to v2025.1:
- Addition of Bader charges and Bader magnetic moments per atom
- Removed a small number of duplicated structures

Stress is reported as a Voigt 6-vector [σxx, σyy, σzz, σxy, σyz, σzx] in kBar
(VASP/pymatgen convention). Converted to 3x3 symmetric matrix in reader.
Bader charges and magnetic moments are stored in configuration info when available.
"""

import json
import os
from pathlib import Path

from ase import Atoms as AseAtoms
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    band_gap_pd,
    cauchy_stress_pd,
    energy_pd,
    formation_energy_pd,
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
    access_key=access_key, access_secret=access_secret, endpoint=endpoint, limit_rows_per_sub_split=250000
)
loader.config_table = "ndb.colabfit.dev.co_matpes_rscan_2"
loader.config_set_table = "ndb.colabfit.dev.cs_matpes_rscan_2"
loader.dataset_table = "ndb.colabfit.dev.ds_matpes_rscan_2"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_matpes_rscan_2"

DATASET_ID = "DS_q1zgft23gdbl_1"
DATASET_NAME = "MatPES-R2SCAN-2025.2"
DESCRIPTION = (
    "MatPES (Materials Potential Energy Surface) is a foundational PES dataset "
    "developed collaboratively by the Materials Virtual Lab and the Materials Project. "
    "The v2025.2 r2SCAN release contains 386,544 structures sampled via the DIRECT method "
    "from 300 K NpT molecular dynamics simulations seeded from Materials Project entries. "
    "Static DFT calculations were performed using VASP with the r2SCAN meta-GGA functional "
    "and MatPESStaticSet convergence settings optimized for energy, force, and stress "
    "calculations. v2025.2 removes a small number of duplicated structures present in v2025.1, "
    "and the original files add Bader charges and Bader magnetic moments per atom. "
    "The previous version of this dataset (MatPES-R2SCAN-2025.1) is available from ColabFit. "
    "There is a companion dataset calculated with the PBE functional "
    "(MatPES-PBE-2025.2)."
)

PUBLICATION = "https://doi.org/10.48550/arXiv.2503.04070"
DATA_LINK = "https://matpes.ai/"
OTHER_LINKS = [
    "https://huggingface.co/datasets/Materialyze/matpes",
    "https://github.com/materialyzeai/matpes",
]
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Aaron D. Kaplan",
    "Runze Liu",
    "Ji Qi",
    "Tsz Wai Ko",
    "Bowen Deng",
    "Janosh Riebesell",
    "Gerbrand Ceder",
    "Kristin A. Persson",
    "Shyue Ping Ong",
]
LICENSE = "BSD-3-Clause"
DOI = None


def voigt_6_to_3x3(voigt):
    """
    Convert Voigt 6-vector stress to 3x3 symmetric matrix.
    VASP/pymatgen convention: [σxx, σyy, σzz, σxy, σyz, σzx]
    """
    s = voigt
    return [
        [s[0], s[3], s[5]],
        [s[3], s[1], s[4]],
        [s[5], s[4], s[2]],
    ]


def reader(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    for record in data:
        struct = record["structure"]
        lattice = struct["lattice"]
        sites = struct["sites"]
        symbols = [site["species"][0]["element"] for site in sites]
        positions = [site["xyz"] for site in sites]
        atoms = AseAtoms(
            symbols=symbols,
            positions=positions,
            cell=lattice["matrix"],
            pbc=lattice["pbc"],
        )
        info = {
            "_name": f"{DATASET_NAME}__{record['matpes_id']}",
            "energy": record["energy"],
            "forces": record["forces"],
            "stress": voigt_6_to_3x3(record["stress"]),
            "bandgap": record["bandgap"],
            "matpes_id": record["matpes_id"],
            "provenance": record["provenance"],
        }
        if record.get("formation_energy_per_atom") is not None:
            info["formation_energy"] = record["formation_energy_per_atom"]
        atoms.info = info
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd, band_gap_pd, formation_energy_pd])

property_map.set_metadata_field("software", "VASP 6.4.x")
property_map.set_metadata_field("method", "DFT-R2SCAN")
property_map.set_metadata_field(
    "input",
    {
        "description": (
            "Static DFT with MatPESStaticSet in pymatgen. VASP r2SCAN meta-GGA "
            "functional with convergence settings optimized for PES training data."
        ),
        "ALGO": "Normal",
        "EDIFF": 1e-5,
        "ENAUG": 1360,
        "ENCUT": 680,
        "METAGGA": "R2SCAN",
        "ISMEAR": 0,
        "ISPIN": 2,
        "KSPACING": 0.22,
        "LAECHG": True,
        "LASPH": True,
        "LCHARG": True,
        "LDAU": False,
        "LMAXMIX": 6,
        "LMIXTAU": True,
        "LORBIT": 11,
        "LREAL": False,
        "LWAVE": False,
        "NELM": 200,
        "NSW": 0,
        "PREC": "Accurate",
        "SIGMA": 0.05,
    },
)
property_map.set_metadata_field("matpes_id", "matpes_id", dynamic=True)
property_map.set_metadata_field("provenance", "provenance", dynamic=True)

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
band_gap_info = PropertyInfo(
    property_name="band-gap",
    field="bandgap",
    units="eV",
    original_file_key="bandgap",
    additional=[("type", {"value": "unknown", "units": None})],
)
formation_energy_info = PropertyInfo(
    property_name="formation-energy",
    field="formation_energy",
    units="eV",
    original_file_key="formation_energy_per_atom",
    additional=[("per-atom", {"value": True, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info, band_gap_info, formation_energy_info])
PROPERTY_MAP = property_map.get_property_map()
print(PROPERTY_MAP)

fp = Path("matpes2/MatPES-R2SCAN-2025.2-charges.json")
gen = reader(fp)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd, band_gap_pd, formation_energy_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
    suppress_warnings={"formation-energy"},
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
    date_requested="2026-04-22",
)

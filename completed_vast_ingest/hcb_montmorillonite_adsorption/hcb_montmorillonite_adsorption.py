"""
HCB_Montmorillonite_Adsorption ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures)

File notes
----------
Source: vast_ingest/hcb_montmorillonite_adsorption/hcb_montmorillonite_adsorption
Each HCB_Mt_<n> subdirectory holds a VASP relaxation (vasprun.xml + CONTCAR)
of a hexachlorobenzene (C6Cl6, HCB) molecule co-adsorbed with water on a
montmorillonite clay surface, probing the effect of partial hydration on
adsorption (e.g. SYSTEM "HCB-4H2O-MNT"). The Mt/ and HCB/ sub-subdirectories
hold the isolated montmorillonite-slab and HCB-molecule reference
calculations used for interaction energies. All ionic steps of each
relaxation are ingested. VASP 6.2.0, PBE functional with the
Tkatchenko-Scheffler dispersion correction using iterative Hirshfeld
partitioning (IVDW=21), Gaussian smearing (ISMEAR=0), Gamma-point sampling.
"""

import os
from pathlib import Path

from ase.io import iread
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
loader.config_table = "ndb.colabfit.dev.co_hcb_mt"
loader.config_set_table = "ndb.colabfit.dev.cs_hcb_mt"
loader.dataset_table = "ndb.colabfit.dev.ds_hcb_mt"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_hcb_mt"

DATASET_ID = "DS_7csieki2zban_0"
DATASET_NAME = "HCB_Montmorillonite_Adsorption"
DESCRIPTION = (
    "Density functional theory study of the adsorption of hexachlorobenzene "
    "(C6Cl6, HCB) on a montmorillonite clay surface and the effect of partial "
    "hydration, with explicit co-adsorbed water molecules. Each configuration "
    "is a VASP geometry optimization of an HCB molecule (with water) on a "
    "montmorillonite slab; all ionic relaxation steps are included. The "
    "dataset also contains the isolated montmorillonite-slab and HCB-molecule "
    "reference calculations used to evaluate interaction energies. "
    "Calculations used VASP 6.2.0 with the PBE functional, the "
    "Tkatchenko-Scheffler dispersion correction with iterative Hirshfeld "
    "partitioning (IVDW=21), Gaussian smearing (ISMEAR=0), and Gamma-point "
    "Brillouin-zone sampling."
)

# Submitted to Applied Surface Science; no article DOI on the Zenodo record yet.
PUBLICATION = None
DATA_LINK = "https://doi.org/10.5281/zenodo.18868464"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Daniel Tunega",
    "Peter Grančič",
    "Martin H. Gerzabek",
    "Leonard Böhm",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "hcb_montmorillonite_adsorption"


def reader(data_path: Path):
    for vasprun in sorted(data_path.rglob("vasprun.xml")):
        system = vasprun.parent.name
        try:
            images = iread(vasprun, index=":", format="vasp-xml")
            for i, atoms in enumerate(images):
                # energy/forces/stress are read from atoms.calc.results,
                # which from_ase preserves; only _name must be set here.
                atoms.info["_name"] = f"{DATASET_NAME}__{system}__{i}"
                yield AtomicConfiguration.from_ase(atoms)
        except Exception as e:
            print(f"Error reading {vasprun}: {e}")
            continue


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP 6.2.0")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "ENCUT": "400 eV",
        "ISMEAR": "0 (Gaussian smearing)",
        "dispersion_correction": (
            "Tkatchenko-Scheffler with iterative Hirshfeld partitioning "
            "(IVDW=21)"
        ),
        "kpoints": "Gamma-point only",
        "description": (
            "VASP 6.2.0, PBE exchange-correlation functional with the "
            "Tkatchenko-Scheffler/iterative-Hirshfeld (TS/HI) dispersion "
            "correction (IVDW=21)."
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
    units="eV/angstrom^3",
    original_file_key="stress",
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
    date_requested="2026-06-02",
)

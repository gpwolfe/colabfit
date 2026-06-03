"""
CoPt_Dry_Reforming_Methane ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures)

File notes
----------
Source: vast_ingest/copt_dry_reforming_methane/copt_dry_reforming_methane
VASP calculations for Co/Pt and CoPt bimetallic catalysts studied for the dry
reforming of methane (DRM). The data are organized into:
  - bulk/                  bulk metals and alloys (Co, Pt, CoPt L1_0, CoPt fcc)
  - slab_models_barriers/  (111) surface slabs of Co, Pt, CoPt
  - energy_barriers/       CH4 and CO2 dissociation reaction paths (ML-NEB)
All ionic steps of every vasprun.xml are ingested. VASP 6.4.2, GGA-PBE
functional with Grimme D3 dispersion and Becke-Johnson damping (IVDW=12),
ENCUT=400 eV (slabs) / 520 eV (bulk), spin-polarized for Co-containing systems,
Methfessel-Paxton smearing (ISMEAR=1), Gamma-centered k-points (k-spacing 1.0
for slabs, 0.66 1/Angstrom for bulk), dipole corrections along the surface
normal for slabs. Transition states located with the CatLearn ML-NEB module.

Published in Journal of Catalysis 457 (2026) 116819.
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
loader.config_table = "ndb.colabfit.dev.co_copt_drm"
loader.config_set_table = "ndb.colabfit.dev.cs_copt_drm"
loader.dataset_table = "ndb.colabfit.dev.ds_copt_drm"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_copt_drm"

DATASET_ID = "DS_uy3ert1cn17o_0"
DATASET_NAME = "CoPt_Dry_Reforming_Methane"
DESCRIPTION = (
    "Density functional theory dataset for cobalt, platinum, and CoPt "
    "bimetallic catalysts investigated for the dry reforming of methane (DRM). "
    "It comprises bulk metals and alloys (Co, Pt, CoPt L1_0 and fcc), (111) "
    "surface slab models, and minimum-energy reaction paths for CH4 and CO2 "
    "dissociation obtained with the machine-learning nudged elastic band "
    "(ML-NEB) method. All ionic relaxation/path images are included. "
    "Calculations used VASP 6.4.2 with the GGA-PBE functional, Grimme D3 "
    "dispersion with Becke-Johnson damping (IVDW=12), a plane-wave cutoff of "
    "400 eV for slabs and 520 eV for bulk, spin polarization for "
    "cobalt-containing systems, Methfessel-Paxton smearing (ISMEAR=1), "
    "Gamma-centered k-point meshes, and dipole corrections normal to the "
    "surfaces. Transition states were located with the CatLearn ML-NEB "
    "module. Configuration sets separate bulk structures, surface slabs, and "
    "reaction-barrier images."
)

PUBLICATION = "https://doi.org/10.1016/j.jcat.2026.116819"
DATA_LINK = "https://doi.org/10.5281/zenodo.17930461"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "David Niedbalka",
    "Hector Prats",
    "Estefanía Díaz López",
    "Marcel Janák",
    "Diana Piankova",
    "Anna Loiudice",
    "Raffaella Buonsanti",
    "Aleix Comas-Vives",
    "Christoph R. Müller",
    "Paula M. Abdala",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "copt_dry_reforming_methane"


def reader(data_path: Path):
    for vasprun in sorted(data_path.rglob("vasprun.xml")):
        rel = vasprun.parent.relative_to(data_path)
        tag = str(rel).replace("/", "__")
        if str(rel).startswith("bulk"):
            category = "bulk"
        elif str(rel).startswith("slab_models_barriers"):
            category = "slab"
        elif str(rel).startswith("energy_barriers"):
            category = "barrier"
        else:
            category = "other"
        try:
            images = iread(vasprun, index=":", format="vasp-xml")
            for i, atoms in enumerate(images):
                # energy/forces/stress are read from atoms.calc.results
                atoms.info["_name"] = f"{DATASET_NAME}__{category}__{tag}__{i}"
                atoms.info["category"] = category
                yield AtomicConfiguration.from_ase(atoms)
        except Exception as e:
            print(f"Error reading {vasprun}: {e}")
            continue


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP 6.4.2")
property_map.set_metadata_field("method", "DFT-PBE+D3(BJ)")
property_map.set_metadata_field(
    "input",
    {
        "ENCUT": "400 eV (slabs), 520 eV (bulk)",
        "EDIFF": "1e-5 eV (slabs), 1e-6 eV (bulk)",
        "EDIFFG": "-0.01 eV/Angstrom (force convergence)",
        "ISPIN": "2 (spin-polarized for cobalt-containing systems)",
        "ISMEAR": "1 (Methfessel-Paxton)",
        "kpoints": "Gamma-centered; k-spacing 1.0 (slabs), 0.66 1/Angstrom (bulk)",
        "dispersion_correction": "Grimme D3 with Becke-Johnson damping (IVDW=12)",
        "dipole_correction": "enabled for slab calculations (IDIPOL=3)",
        "pseudopotentials": "PAW",
        "transition_states": "CatLearn ML-NEB",
        "description": (
            "VASP 6.4.2, GGA-PBE exchange-correlation functional with Grimme "
            "D3-BJ dispersion; 400 eV (slabs) / 520 eV (bulk) plane-wave "
            "cutoff; PAW pseudopotentials; spin-polarized for Co-containing "
            "systems; dipole correction along the surface normal for slabs."
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

dm.create_configuration_sets(
    loader=loader,
    name_label_match=[
        (
            f"{DATASET_NAME}__bulk__.*",
            None,
            f"{DATASET_NAME}_bulk",
            "Bulk metal and alloy structures (Co, Pt, CoPt L1_0 and fcc).",
            False,
        ),
        (
            f"{DATASET_NAME}__slab__.*",
            None,
            f"{DATASET_NAME}_slabs",
            "(111) surface slab models of Co, Pt, and CoPt.",
            False,
        ),
        (
            f"{DATASET_NAME}__barrier__.*",
            None,
            f"{DATASET_NAME}_reaction_barriers",
            "CH4 and CO2 dissociation reaction-path images (ML-NEB).",
            False,
        ),
    ],
)

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

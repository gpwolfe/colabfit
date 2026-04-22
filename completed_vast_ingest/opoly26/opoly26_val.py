"""
Open Polymers 2026 (OPoly26) — validation set ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Å

File notes
----------
Source: vast_ingest/opoly26/val/ (.aselmdb LMDB files)
Environment: .fairchem_env (fairchem-core module required for reading)

OPoly26 contains over 6.57 million DFT calculations on polymer cluster fragments
of up to 360 atoms, derived from copolymer systems with diverse monomer compositions,
polymerization degrees, chain architectures, and solvation environments. Calculations
were performed at the B97M-V/def2-SVP level of theory using ORCA. The dataset is
intended to support machine learning model development for polymer property prediction.

Publication: https://arxiv.org/abs/2512.23117
"""

import os
from pathlib import Path

from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyInfo, PropertyMap
from dotenv import load_dotenv
from fairchem.core.datasets import AseDBDataset
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
loader.config_table = "ndb.colabfit.dev.co_opoly26_val"
loader.config_set_table = "ndb.colabfit.dev.cs_opoly26_val"
loader.dataset_table = "ndb.colabfit.dev.ds_opoly26_val"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_opoly26_val"

DATASET_ID = "DS_oinzbpqoh8c8_0"
DATASET_NAME = "OPoly26-val"
DESCRIPTION = (
    "Validation set of the Open Polymers 2026 (OPoly26) dataset. OPoly26 contains "
    "over 6.57 million density functional theory (DFT) calculations on cluster "
    "fragments of up to 360 atoms derived from polymeric systems. "
    "The dataset encompasses variations in monomer "
    "composition, polymerization degree, chain architectures, and solvation "
    "environments to improve machine learning model performance for polymer "
    "property prediction. Calculations were performed at the B97M-V/def2-SVP "
    "level of theory using ORCA."
)

PUBLICATION = "https://doi.org/10.48550/arXiv.2512.23117"
DATA_LINK = "https://huggingface.co/facebook/OMol25"
PUBLICATION_YEAR = "2026"
OTHER_LINKS = None
AUTHORS = [
    "Daniel S. Levine",
    "Nicholas Liesen",
    "Lauren Chua",
    "James Diffenderfer",
    "Helgi I. Ingolfsson",
    "Matthew P. Kroonblawd",
    "Nitesh Kumar",
    "Amitesh Maiti",
    "Supun S. Mohottalalage",
    "Muhammed Shuaibi",
    "Brian Van Essen",
    "Brandon M. Wood",
    "C. Lawrence Zitnick",
    "Samuel M. Blau",
    "Evan R. Antoniuk",
]
LICENSE = "FAIR Chemistry License"
DOI = None

DATA_PATH = Path("opoly26/val")


def reader(ds_path):
    dataset = AseDBDataset({"src": str(ds_path)})
    for i in range(len(dataset)):
        atoms = dataset.get_atoms(i)
        source = atoms.info.get("source", "")
        atoms.info = {
            "_name": f"{DATASET_NAME}__{source}",
            "source": source,
        }
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "ORCA")
property_map.set_metadata_field("method", "DFT-ωB97M-V")
property_map.set_metadata_field(
    "input",
    {
        "basis_set": "def2-TZVPD",
        "description": (
            "DFT calculation with ωB97M-V meta-GGA functional and def2-TZVPD basis set "
            "with non-local dispersion correction (NL). Integral acceleration via RI-J "
            "and COSX. Integral threshold (thresh) set to 1e-12 and primitive batch "
            "threshold (tcut) set to 1e-13. Tight convergence settings. DEFGRID3 used "
            "for exchange-correlation (590 angular points) and COSX "
            "(302 angular points for the final grid) to ensure consistency "
            "between energy and forces."
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
print(PROPERTY_MAP)

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
    date_requested="2026-04-09",
)

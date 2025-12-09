"""
VQM24 is split into an "all", a "stable conformers", a "saddle" and a "DMC" (monte carlo)
split, where the non-dmc are all dft calculations.
"""

import os
from pathlib import Path

import numpy as np
from ase.atoms import Atoms
from colabfit.tools.property_definitions import atomization_energy_pd, energy_pd
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyInfo, PropertyMap
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .getOrCreate()
)

loader = VastDataLoader(
    spark_session=spark,
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
loader.config_table = "ndb.colabfit.dev.co_vqm"
loader.config_set_table = "ndb.colabfit.dev.cs_vqm"
loader.dataset_table = "ndb.colabfit.dev.ds_vqm"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_vqm"

DATASET_ID = "DS_typ49f9r0b6v_0"
DATASET_NAME = "Vector-QM24_DFT_all"
DESCRIPTION = "All structures calculated for Vector-QM24 (VQM24) with properties calculated using DFT. Vector-QM24 is a quantum chemistry dataset of ~836 thousand small organic and inorganic molecules. Dataset covers all possible neutral closed-shell small organic and inorganic molecules with up to five heavy (p-block) atoms: C, N, O, F, Si, P, S, Cl, Br."  # noqa: E501

PUBLICATION = "https://doi.org/10.1038/s41597-025-05428-4"
DATA_LINK = "https://doi.org/10.5281/zenodo.15442257"
OTHER_LINKS = ["https://github.com/dkhan42/VQM24"]
PUBLICATION_YEAR = "2025"

AUTHORS = [
    "Danish Khan",
    "Anouar Benali",
    "Scott Y. H. Kim",
    "Guido Falk von Rudorff",
    "O. Anatole von Lilienfeld",
]
LICENSE = "CC-BY-4.0"
DOI = None

# energy reported in Hartree
# DFT methods
# ωB97X-D3/cc-pVDZ
# Psi4

# DMC methods
# QMCPACK


def reader(filepath):
    with np.load(filepath, allow_pickle=True) as f:
        coords = f["coordinates"]
        numbers = f["atoms"]
        energies = f["Etot"]
        atomization_energies = f["Eatomization"]
    for i, coords in enumerate(coords):
        atoms = Atoms(
            positions=coords,
            numbers=numbers[i],
            pbc=False,
        )
        atoms.info = {
            "_name": f"{DATASET_NAME}__{filepath.stem}__{i}",
            "energy": energies[i],
            "atomization_energy": atomization_energies[i],
        }
        yield AtomicConfiguration.from_ase(atoms)


property_map_dft = PropertyMap([energy_pd, atomization_energy_pd])

property_map_dft.set_metadata_field("software", "Psi4")
property_map_dft.set_metadata_field("method", "DFT-ωB97X+D3")
property_map_dft.set_metadata_field(
    "input",
    {
        "basis_set": "cc-pvdz",
        "g_convergence": "GAU_TIGHT",
        "dft_radial_points": 99,
        "dft_spherical_points": 590,
    },
)


energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units={"source-unit": {"value": "Hartree"}},
    original_file_key="Etot",
)
atomization_energy_info = PropertyInfo(
    property_name="atomization-energy",
    field="atomization_energy",
    units={"source-unit": {"value": "Hartree"}},
    original_file_key="Eatomization",
)
property_map_dft.set_properties([energy_info, atomization_energy_info])
PROPERTY_MAP = property_map_dft.get_property_map()
print(PROPERTY_MAP)

fp = Path("DFT_all.npz")
gen = reader(fp)


dm = DataManager(
    prop_defs=[energy_pd, atomization_energy_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=False,
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
    date_requested="2025-05-19",
)

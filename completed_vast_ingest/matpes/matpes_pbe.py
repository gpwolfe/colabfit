"""
MatPES-PBE-2025.1 ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Å
cauchy-stress: stress tensor in kBar (Voigt 6-vector), converted to eV/Å³

File notes
----------
Source: MatPES-PBE-2025.1.json
434,712 structures sampled via the DIRECT method from 300 K NpT MD simulations
seeded from Materials Project entries. Static DFT calculations performed using
VASP with the PBE functional and MatPESStaticSet convergence settings.

Stress is reported as a Voigt 6-vector [σxx, σyy, σzz, σxy, σyz, σzx] in kBar
(VASP/pymatgen convention). Converted to 3x3 symmetric matrix in reader.
"""

import json
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
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("MatPES_PBE")
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
loader.config_table = "ndb.colabfit.dev.co_matpes_pbe"
loader.config_set_table = "ndb.colabfit.dev.cs_matpes_pbe"
loader.dataset_table = "ndb.colabfit.dev.ds_matpes_pbe"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_matpes_pbe"

DATASET_ID = "DS_9xi183g4pavv_0"
DATASET_NAME = "MatPES-PBE-2025.1"
DESCRIPTION = (
    "MatPES (Materials Potential Energy Surface) is a foundational PES dataset "
    "developed collaboratively by the Materials Virtual Lab and "
    "Materials Project. The v2025.1 PBE release contains 434,712 structures sampled "
    "via the DIRECT method from 300 K NpT molecular dynamics simulations seeded from "
    "Materials Project entries. Static DFT calculations were performed using VASP with "
    "the PBE functional and MatPESStaticSet convergence settings optimized for "
    "energy, force, and stress calculations. There is a companion dataset calculated "
    "with the r2SCAN functional (MatPES-R2SCAN-2025.1)."
)

PUBLICATION = "https://doi.org/10.48550/arXiv.2503.04070"
DATA_LINK = "https://matpes.ai/"
OTHER_LINKS = [
    "https://huggingface.co/datasets/mavrl/matpes",
    "https://github.com/materialyzeai/matpes",
]
PUBLICATION_YEAR = "2025"

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
        atoms.info = {
            "_name": f"{DATASET_NAME}__{record['matpes_id']}",
            "energy": record["energy"],
            "forces": record["forces"],
            "stress": voigt_6_to_3x3(record["stress"]),
        }
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP 6.4.x")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "description": (
            "Static DFT with MatPESStaticSet in pymatgen. VASP PBE functional with "
            "convergence settings optimized for PES training data."
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
print(PROPERTY_MAP)

fp = Path("/home/gpwolfe/colabfit/MatPES-PBE-2025.1.json")
gen = reader(fp)

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
    date_requested="2026-02-24",
)

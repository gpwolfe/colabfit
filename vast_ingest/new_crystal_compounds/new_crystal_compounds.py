"""
Files are pymatgen saved as json. To create ase from these, uses pymatgen ase
adapter with some additions to the process in order to gather energies, etc.
Energy is taken from corrected energy. band gap chosen is direct.
"""

import json
import logging
import os
from pathlib import Path

from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    band_gap_pd,
    cauchy_stress_pd,
    energy_above_hull_pd,
    energy_pd,
    formation_energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from monty.json import MontyDecoder
from pymatgen.io.ase import AseAtomsAdaptor
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)
# TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .config("spark.task.maxFailures", 1)
    .getOrCreate()
)

access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

loader = VastDataLoader(
    spark_session=spark,
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)

loader.config_table = "ndb.colabfit.dev.co_ncc"
loader.config_set_table = "ndb.colabfit.dev.cs_ncc"
loader.dataset_table = "ndb.colabfit.dev.ds_ncc"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_ncc"


DATASET_FP = Path("data")
DATASET_NAME = "Finding_new_crystal_compounds_using_chemical_similarity"
DATASET_ID = "DS_sk8zwvk3qxur_0"
DESCRIPTION = "This is the dataset from npj Comp. Mater 7, 12 (2021), 'Predicting stable crystalline compounds using chemical similarity'. Stable crystal structure compositions of up to 12 atoms were gathered from the Materials Project database. These structures were mutated by replacing all of a given element with a similar element (see publication for details)."  # noqa: E501
PUBLICATION = "https://doi.org/10.1038/s41524-020-00481-6"
DATA_LINK = "https://alexandria.icams.rub.de/"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = ["Hai-Chen Wang", "Silvana Botti", "Miguel A. L. Marques"]
LICENSE = "CC-BY-4.0"
DOI = None

property_map = PropertyMap(
    [
        energy_pd,
        atomic_forces_pd,
        band_gap_pd,
        energy_above_hull_pd,
        formation_energy_pd,
        cauchy_stress_pd,
    ]
)
property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "kpoints": "gamma-centered 1000/reciprocal atom",
        "PAW-cutoff": "520 eV",
        "energy-cutoff": "2 meV/atom",
        "forces-cutoff": "0.005 eV/A",
        "band-structure-kpoints": "5000 points per atom",
    },
)

energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy_corrected",
    additional=[("per-atom", {"value": False, "units": None})],
)
stress_info = property_info(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
atomic_forces_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)
band_gap_info = property_info(
    property_name="band-gap",
    field="band_gap_dir",
    units="eV",
    original_file_key="band_gap_dir",
    additional=[("type", {"value": "direct", "units": None})],
)
energy_above_hull_info = property_info(
    property_name="energy-above-hull",
    field="energy_above_hull",
    units="eV",
    original_file_key="e_above_hull",
    additional=[("per-atom", {"value": False, "units": None})],
)
formation_energy_info = property_info(
    property_name="formation-energy",
    field="formation_energy",
    units="eV",
    original_file_key="e_form",
    additional=[("per-atom", {"value": False, "units": None})],
)
property_map.set_properties(
    [
        energy_info,
        stress_info,
        atomic_forces_info,
        band_gap_info,
        energy_above_hull_info,
        formation_energy_info,
    ]
)
PROPERTY_MAP = property_map.get_property_map()
PROPERTY_MAP


def read_json(fp):
    with open(fp, "r") as f:
        subs = json.load(f, cls=MontyDecoder)
    ad = AseAtomsAdaptor()
    for i, s in enumerate(subs["entries"]):
        info = {}
        info["band_gap_dir"] = s.data["band_gap_dir"]
        info["energy"] = s.data["energy_corrected"]
        info["energy_above_hull"] = s.data["e_above_hull"]
        info["formation_energy"] = s.data["e_form"]
        info["stress"] = s.data.get("stress")
        info["_name"] = f"{DATASET_NAME}__file_{fp.stem}__ix_{i}__id_{s.data['mat_id']}"
        c = ad.get_atoms(s.structure)
        info["forces"] = c.arrays.get("forces")
        c = AtomicConfiguration.from_ase(c, info=info)
        yield c


def read_wrapper(data_dir):
    fps = sorted(list(Path(data_dir).glob("*.json")))
    for i, fp in enumerate(fps):
        logger.info(f"Reading file {i}: {fp}")
        yield from read_json(fp)
        logger.info(f"Finished file {i}: {fp}")


dm = DataManager(
    prop_defs=[
        energy_pd,
        atomic_forces_pd,
        band_gap_pd,
        cauchy_stress_pd,
        energy_above_hull_pd,
        formation_energy_pd,
    ],
    configs=read_wrapper(DATASET_FP),
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=50000,
)

print(dm.co_po_example_rows())

# dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)
# dm.create_dataset(
#     loader=loader,
#     name=DATASET_NAME,
#     authors=AUTHORS,
#     publication_link=PUBLICATION,
#     data_link=DATA_LINK,
#     other_links=OTHER_LINKS,
#     description=DESCRIPTION,
#     data_license=LICENSE,
#     publication_year=PUBLICATION_YEAR,
# )

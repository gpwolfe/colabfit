import logging
import os
from pathlib import Path

import h5py
from ase.atoms import Atoms
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, PropertyInfo
from colabfit.tools.property_definitions import energy_pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)
# TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_ui_port = os.getenv("__SPARK_UI_PORT")

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", jars)
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .config("spark.task.maxFailures", 1)
    .getOrCreate()
)
loader = VastDataLoader(
    spark_session=spark,
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)


loader.config_table = "ndb.colabfit.dev.co_qmc"
loader.config_set_table = "ndb.colabfit.dev.cs_qmc"
loader.dataset_table = "ndb.colabfit.dev.ds_qmc"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_qmc"


DATASET_FP = Path("data/QMC_BLG_hBN_structures.hdf")
DATASET_ID = "DS_nurmkg9ihy1j_0"
DATASET_NAME = "Graphene-hBN_and_Graphene-Graphene_QMC"
DESCRIPTION = "The QMC-calculated split of the Graphene-hBN_and_Graphene-Graphene dataset. This dataset family (see other Graphene-hBN_and_Graphene_Graphene datasets) contains data for Graphene-Graphene and Graphene-hexagonal boron nitride (hBN) ab initio calculations for structures with different interlayer distances and disregistries, calculated using DFT with D2 van der Waals corrections, DFT with D3 van der Waals corrections, and QMC methods."  # noqa: E501
PUBLICATION = "https://doi.org/10.1103/xkwm-zd77"
DATA_LINK = None
OTHER_LINKS = ["https://doi.org/10.18126/wms3-v894"]
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Kittithat Krongchon",
    "Lucas K. Wagner",
    "Tawfiqur Rakib",
    "Daniel Palmer",
    "Elif Ertekin",
    "Harley T. Johnson",
]
LICENSE = "CC-BY-4.0"
DOI = None


def read_hdf(hdf_name):
    hdf_path = Path(hdf_name)
    with h5py.File(hdf_name) as hdf:
        latvecs = hdf["latvecs"][...]
        coords = hdf["coords"][...]
        atomic_numbers = hdf["atomic_numbers"][...]
        energy = hdf["energy"][...]
        energy_err = hdf["energy_err"][...]
        distance = hdf["distance"][...]
        ntiling = hdf["ntiling"][...]
        registry = hdf["registry"][...]
        for i in range(len(coords)):
            atoms = Atoms(
                cell=latvecs[i],
                numbers=atomic_numbers[i],
                positions=coords[i],
                pbc=(1, 1, 0),
            )
            info = {
                "energy": energy[i],
                "energy_err": energy_err[i],
                "interlayer_distance": distance[i],
                "ntiling": ntiling[i],
                "registry": registry[i],
                "_name": f"{hdf_path.stem}_{i}",
            }
            yield AtomicConfiguration.from_ase(atoms, info=info)


property_map = PropertyMap([energy_pd])
property_map.set_metadata_field("software", "QMCPACK")
property_map.set_metadata_field("method", "IP-QMC")
property_map.set_metadata_field(
    "input",
    {
        "k-grid": "4x4x1",
        "wave-function": "Slater-Jastrow",
        "boundary_condition": "3D, twisted, 40-angstrom cell height",
    },
)
energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", False)],
)

for key in ["energy_err", "ntiling", "interlayer_distance", "registry"]:
    property_map.set_metadata_field(key, key, dynamic=True)
property_map.set_properties([energy_info])
PM = property_map.get_property_map()

print(PM)


dm = DataManager(
    prop_defs=[energy_pd],
    configs=read_hdf(DATASET_FP),
    prop_map=PM,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
)
dm.load_co_po_to_vastdb(loader, batching_ingest=True)
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
    equilibrium=True,
    date_requested="2025-10-01",
)

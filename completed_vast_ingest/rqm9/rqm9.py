""" """

import logging
import os
from pathlib import Path

from ase.atoms import Atoms

from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

load_dotenv()
logger = logging.getLogger(__name__)
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_ui_port = os.getenv("__SPARK_UI_PORT")

spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", jars)
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .config("spark.task.maxFailures", 1)
    .getOrCreate()
)


access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

loader = VastDataLoader(
    spark_session=spark,
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)

loader.config_table = "ndb.colabfit.dev.co_rqm9"
loader.config_set_table = "ndb.colabfit.dev.cs_rqm9"
loader.dataset_table = "ndb.colabfit.dev.ds_rqm9"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_rqm9"

DATASET_FP = Path("rQM9.sdf")
DATASET_ID = "DS_qpqczxvnj5bo_0"
DATASET_NAME = "rQM9"
DESCRIPTION = "133885 molecular structures from the QM9 with revised bond and charges in the SDF format. Bond information can be gathered from the metadata column of the parquet files, a map where the key bonds contains the bond indices as they appear in the final rows of an SDF molecule block. If additional charges are present, these are contained under the key charge_info. rQM9 is derived from DeepChem's QM9 SDF dataset and rectifies the original dataset's net-charge discrepancies and invalid bond orders by enforcing correct valency-charge configurations. Nevertheless, a subset of molecules remains problematic, as they either fail RDKit sanitization or fragment into multiple components. The zero-based indices of these unresolved molecules are provided in a NumPy file in the original data file."  # noqa: E501
PUBLICATION = "https://doi.org/10.48550/arXiv.2505.21469"
DATA_LINK = "https://huggingface.co/datasets/colabfit/rQM9"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Cheng Zeng",
    "Jirui Jin",
    "George Karypis",
    "Mark Transtrum",
    "Ellad B. Tadmor",
    "Richard G. Hennig",
    "Adrian Roitberg",
    "Stefano Martiniani",
    "Mingjie Liu",
]
LICENSE = "CC-BY-4.0"
DATE_REQUESTED = "2025-05-27"
DOI = None


def get_num_atoms_sdf_v2000(first_line: str) -> int:
    return int(first_line[0:3].split(" ")[0])


property_map = PropertyMap([])
property_map.set_metadata_field("software", "Gaussian 09")
property_map.set_metadata_field("method", "DFT-B3LYP")
property_map.set_metadata_field(
    "input", "Reassigned bond orders or charges to enforce valency-charge consistency"
)
property_map.set_metadata_field("bonds", "bonds", dynamic=True)
property_map.set_metadata_field("charge_info", "charge_info", dynamic=True)
PROPERTY_MAP = property_map.get_property_map()

print(PROPERTY_MAP._metadata)


def make_atoms(block):
    symbols = []
    positions = []
    bonds = []
    charge_info = []
    name = f"rQM9_{block[0]}"
    num_atoms = get_num_atoms_sdf_v2000(block[3])
    atom_block = block[4 : 4 + num_atoms]  # noqa: E203
    for i in range(num_atoms):
        line1 = atom_block[i].split()
        positions.append(line1[0:3])
        symbols.append(line1[3])
    for line2 in block[4 + num_atoms :]:  # noqa: E203
        if line2.startswith("M  END"):
            info = {"_name": name, "bonds": bonds}
            if len(charge_info) > 0:
                info["charge_info"] = charge_info
            return AtomicConfiguration.from_ase(
                Atoms(
                    symbols=symbols,
                    positions=positions,
                    cell=None,
                    pbc=False,
                    info=info,
                )
            )
        elif line2.startswith("M"):
            charge_info.append(line2)
        else:
            bonds.append([int(x) for x in line2.split()])


def read_sdf(file_obj):
    with open(file_obj, "r") as f:
        lines = f.readlines()
    block = []
    for line in lines:
        if line.startswith("$$$$"):
            yield make_atoms(block)
            block = []
        else:
            block.append(line.strip())


dm = DataManager(
    prop_defs=None,
    configs=read_sdf(DATASET_FP),
    prop_map=PM,
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
    equilibrium=True,
    date_requested=DATE_REQUESTED,
)

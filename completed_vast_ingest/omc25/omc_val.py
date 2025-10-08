"""
fairchem.core.datasets.AseDBDataset files
metadata...npz files contain only number of atoms per configuration
"""

import logging
import os
from pathlib import Path

from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from fairchem.core.datasets import AseDBDataset
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

loader.config_table = "ndb.colabfit.dev.co_omc_val"
loader.config_set_table = "ndb.colabfit.dev.cs_omc_val"
loader.dataset_table = "ndb.colabfit.dev.ds_omc_val"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_omc_val"


DATASET_FP = Path("omc_val_250802/val")
DATASET_ID = "DS_beoiy4mqhhex_0"
DATASET_NAME = "Open_Molecular_Crystals_2025_OMC25_validation"
DESCRIPTION = "The validation split of OMC25. Open Molecular Crystals 2025 (OMC25) is a molecular crystal dataset produced by Meta. The OE62 dataset was used as a source for sampling molecules; crystals were generated with Genarris 3.0; from these, relaxation trajectories were generated and sampled to create the final dataset. See the publication for details."  # noqa: E501
PUBLICATION = "https://doi.org/10.48550/arXiv.2508.02651"
DATA_LINK = "https://huggingface.co/facebook/OMC25"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Vahe Gharakhanyan",
    "Luis Barroso-Luque",
    "Yi Yang",
    "Muhammed Shuaibi",
    "Kyle Michel",
    "Daniel S. Levine",
    "Misko Dzamba",
    "Xiang Fu",
    "Meng Gao",
    "Xingyu Liu",
    "Haoran Ni",
    "Keian Noori",
    "Brandon M. Wood",
    "Matt Uyttendaele",
    "Arman Boromand",
    "C. Lawrence Zitnick",
    "Noa Marom",
    "Zachary W. Ulissi",
    "Anuroop Sriram",
]
LICENSE = "CC-BY-4.0"
DOI = None


def reader(fp: Path):
    dataset = AseDBDataset({"src": str(fp)})
    for i in dataset.indices:
        atoms = dataset.get_atoms(i)
        atoms.info["cauchy_stress"] = atoms.get_stress(voigt=False)
        atoms.info["_name"] = f"{DATASET_NAME}_index_{i}"
        c = AtomicConfiguration.from_ase(atoms)
        yield c


property_map = PropertyMap(
    [
        energy_pd,
        atomic_forces_pd,
        cauchy_stress_pd,
    ]
)
property_map.set_metadata_field("software", "VASP 6.3")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "EDIFF": "1e-06",
        "EDIFFG": "-0.001",
        "ENAUG": "1360",
        "ENCUT": "520",
        "KPOINTS": "gamma-centered, pymatgen default",
    },
)

energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
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


property_map.set_properties(
    [
        energy_info,
        stress_info,
        atomic_forces_info,
    ]
)
PROPERTY_MAP = property_map.get_property_map()
print(PROPERTY_MAP)


dm = DataManager(
    prop_defs=[
        energy_pd,
        atomic_forces_pd,
        cauchy_stress_pd,
    ],
    configs=reader(DATASET_FP),
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=50000,
)

# print(dm.co_po_example_rows())

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
)

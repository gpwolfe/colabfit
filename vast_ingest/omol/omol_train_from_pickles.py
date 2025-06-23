"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------


File notes
----------

"""

import logging
import os
from pathlib import Path
from time import time
from pickle import load

import pyspark
from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession

logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")

logging.info(f"pyspark version: {pyspark.__version__}")
load_dotenv()
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
if SLURM_TASK_ID is None:
    raise ValueError(
        "SLURM_ARRAY_TASK_ID is not set. Please set it to a valid task ID."
    )
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
    .config("spark.ui.showConsoleProgress", "true")
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)


loader = VastDataLoader(
    spark_session=spark_session,
    access_key=os.getenv("SPARK_ID"),
    access_secret=os.getenv("SPARK_KEY"),
    endpoint=os.getenv("SPARK_ENDPOINT"),
)


# loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_omol_train_mp_longer"
loader.prop_object_table = "ndb.colabfit.dev.po_omol_train_mp_longer"
loader.config_set_table = "ndb.colabfit.dev.cs_omol_train_mp_longer"
loader.dataset_table = "ndb.colabfit.dev.ds_omol_train_mp_longer"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_omol_train_mp_longer"


logging.info(loader.config_table)
logging.info(loader.config_set_table)
logging.info(loader.dataset_table)
logging.info(loader.prop_object_table)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/omol/omol_train_pickles"  # noqa
)
DATASET_NAME = "OMol25_train"
DATASET_ID = "DS_bzcf331ql8ji_0"
DESCRIPTION = "The full-size training set from OMol25. From the dataset creator: OMol25 represents the largest high quality molecular DFT dataset spanning biomolecules, metal complexes, electrolytes, and community datasets. OMol25 was generated at the ω B97M-V/def2-TZVPD level of theory."  # noqa
DOI = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Daniel S. Levine",
    "Muhammed Shuaibi",
    "Evan Walter Clark Spotte-Smith",
    "Michael G. Taylor",
    "Muhammad R. Hasyim",
    "Kyle Michel",
    "Ilyes Batatia",
    "Gábor Csányi",
    "Misko Dzamba",
    "Peter Eastman",
    "Nathan C. Frey",
    "Xiang Fu",
    "Vahe Gharakhanyan",
    "Aditi S. Krishnapriyan",
    "Joshua A. Rackers",
    "Sanjeev Raja",
    "Ammar Rizvi",
    "Andrew S. Rosen",
    "Zachary Ulissi",
    "Santiago Vargas",
    "C. Lawrence Zitnick",
    "Samuel M. Blau",
    "Brandon M. Wood",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.48550/arXiv.2505.08762"
DATA_LINK = "https://huggingface.co/facebook/OMol25"


property_map = PropertyMap([atomic_forces_pd, energy_pd])
property_map.set_metadata_field("software", "ORCA")
property_map.set_metadata_field("method", "ωB97M-V")
property_map.set_metadata_field("basis_set", "def2-TZVPD")
property_map.set_metadata_field("input", "RI-J, COSX, DEFGRID3")

for field in [
    "source",  # Unique internal identifier
    "reference_source",  # Internal identifier
    "data_id",  # Dataset domain
    "charge",  # Total charge
    "spin",  # Total spin
    "num_atoms",  # Total number of atoms
    "num_electrons",  # Total number of electrons
    "num_ecp_electrons",  # Total number of effective core potential electrons
    "n_scf_steps",  # Total number of self-consistent DFT steps
    "n_basis",  # Number of basis functions
    "unrestricted",  # Restricted or unrestricted flag
    "nl_energy",  # Dispersion energy (VV10)
    "integrated_densities",  # Integral of the electron density, (alpha, beta, total)
    "homo_energy",  # HOMO energy (eV)
    "homo_lumo_gap",  # HOMO-LUMO gap
    "s_squared",  # S^2 reports on the total net magnetization (how many electrons are unpaired)
    "s_squared_dev",  # Deviation of S^2 from ideal
    "warnings",  # List of DFT ORCA warning messages
    "mulliken_charges",  # Partial mulliken charges
    "lowdin_charges",  # Partial lowdin charges
    "nbo_charges",  # Partial nbo charges, if available
    "composition",  # Composition
    # if unrestricted,
    "mulliken_spins",  # Partial mulliken spins
    "lowdin_spins",  # Partial lowdin spins
    "nbo_spins",  # Partial nbo spins, if available
]:
    property_map.set_metadata_field(field, field, dynamic=True)


energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)

property_map.set_properties([energy_info, force_info])

PROPERTY_MAP = property_map.get_property_map()


def reader(fp):
    with open(fp, "rb") as f:
        atoms = load(f)
        if not isinstance(atoms, list):
            atoms = [atoms]
        assert isinstance(
            atoms[0], AtomicConfiguration
        ), f"Expected list of AtomicConfiguration, got {type(atoms[0])}."
        for a in atoms:
            yield a


def read_wrapper(fdir):
    fps = sorted(list(fdir.glob("*")))
    total_num_files = 5120
    nbatches = 1000
    batches = [
        fps[i : i + total_num_files // nbatches]
        for i in range(0, total_num_files, total_num_files // nbatches)
    ]
    if SLURM_TASK_ID >= len(batches):
        raise ValueError(
            f"SLURM_TASK_ID {SLURM_TASK_ID} is out of range for the number of batches {len(batches)}."  # noqa
        )
    logging.info(f"SLURM_TASK_ID: {SLURM_TASK_ID}")
    batch_fps = batches[SLURM_TASK_ID]
    logging.info(f"Processing batch {SLURM_TASK_ID} with {len(batch_fps)} files.")
    logging.info(f"Batch file paths: {batch_fps}")
    for fp in batch_fps:
        logging.info(f"Processing file {fp}")
        logging.info(f"Reading {fp}")
        yield from reader(fp)


def main():
    t0 = time()
    config_generator = read_wrapper(DATASET_FP)
    logging.info("Creating DataManager")
    dm = DataManager(
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=10000,
    )
    logging.info(f"Time to prep: {time() - t0}")
    t1 = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=True)
    t2 = time()
    logging.info(f"Time to load: {t2 - t1}")
    logging.info(f"Total time: {time() - t0}")
    logging.info("complete")


if __name__ == "__main__":
    main()

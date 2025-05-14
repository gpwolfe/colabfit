"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------


File notes
----------
json files
contains dict where keys = material id
each material id contains list of dicts with (examples):
'kpoints': [7, 2, 2],
'PREC': 'high',
'ENMAX': 264.4175,
'ENAUG': 446.842,
'steps': [this is a list of dicts]

each step contains keys:
['structure', 'energy', 'forces', 'stress']
where structure is a pymatgen.core.structure.Structure object

an ASE Atoms object can be created as follows
Structure.from_dict(data["agm003272798"][1]["steps"][0]["structure"]).to_ase_atoms()

stress units are assumed to be eV/angstrom^3, as pymatgen appears to use
these by default
"""

import os
from pathlib import Path
from time import time
import logging
import pyspark
from colabfit.tools.vast.database import DataManager, VastDataLoader
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")
logger.addHandler(handler)

logging.info(f"pyspark version: {pyspark.__version__}")
load_dotenv()
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")

n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

# spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.executor.memoryOverhead", "600")
    # .config("spark.ui.port", f"{spark_ui_port}")
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


loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_alex"
loader.prop_object_table = "ndb.colabfit.dev.po_alex"
loader.config_set_table = "ndb.colabfit.dev.cs_alex"
loader.dataset_table = "ndb.colabfit.dev.ds_alex"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_alex"


# loader.config_table = "ndb.colabfit.dev.co_wip"
# loader.prop_object_table = "ndb.colabfit.dev.po_wip"
# loader.config_set_table = "ndb.colabfit.dev.cs_wip"
# loader.dataset_table = "ndb.colabfit.dev.ds_wip"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"

logging.info(loader.config_table)
logging.info(loader.config_set_table)
logging.info(loader.dataset_table)
logging.info(loader.prop_object_table)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alex_1d/alexandria_1d_pickles"  # noqa
)
DATASET_NAME = "Alexandria_geometry_optimization_paths_PBE_1D"
DATASET_ID = "DS_xnio123pebli_0"
DESCRIPTION = "The Alexandria Materials Database contains theoretical crystal structures in 1D, 2D and 3D discovered by machine learning approaches using DFT with PBE, PBEsol and SCAN methods. This dataset represents the geometry optimization paths for 1D crystal structures from Alexandria calculated using PBE methods."  # noqa

DOI = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Jonathan Schmidt",
    "Noah Hoffmann",
    "Hai-Chen Wang",
    "Pedro Borlido",
    "Pedro J. M. A. Carriço",
    "Tiago F. T. Cerqueira",
    "Silvana Botti",
    "Miguel A. L. Marques",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.1002/adma.202210788"
DATA_LINK = "https://alexandria.icams.rub.de/"


def main():
    beg = time()
    logger.info("Creating DataManager")
    dm = DataManager(
        dataset_id=DATASET_ID,
    )
    logger.info(f"Time to prep: {time() - beg}")
    logger.info("Creating dataset")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
    )
    logger.info("complete")

    logger.info(f"Time to create dataset: {time() - t}")
    logger.info(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

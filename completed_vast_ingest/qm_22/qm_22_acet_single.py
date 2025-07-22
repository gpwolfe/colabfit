"""
author:gpwolfe

Data can be downloaded from:
https://github.com/jmbowma/QM-22

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------

File notes
----------

"""

import logging
import os
from pathlib import Path

import numpy as np
import pyspark
from colabfit.tools.property_definitions import energy_pd
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.info(f"pyspark version: {pyspark.__version__}")
load_dotenv()
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")

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


loader.config_table = "ndb.colabfit.dev.co_qm22_acet_s"
loader.prop_object_table = "ndb.colabfit.dev.po_qm22_acet_s"
loader.config_set_table = "ndb.colabfit.dev.cs_qm22_acet_s"
loader.dataset_table = "ndb.colabfit.dev.ds_qm22_acet_s"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_qm22"

logging.info(loader.config_table)
logging.info(loader.config_set_table)
logging.info(loader.dataset_table)
logging.info(loader.prop_object_table)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/qm22/Acetaldehyde_singlet.xyz"  # noqa: E501
)
DATASET_NAME = "QM-22_Acetaldehyde_singlet"

DATASET_ID = "DS_82ubxqu96yz7_0"
LICENSE = "Apache-2.0"
PUBLICATION = "https://doi.org/10.1021/jz200719x"
DATA_LINK = "https://github.com/jmbowma/QM-22"
OTHER_LINKS = ["https://doi.org/10.1063/5.0089200"]
PUBLICATION_YEAR = "2025"

AUTHORS = [
    "Yong-Chang Han",
    "Benjamin C. Shepler",
    "Joel M. Bowman",
]
DESCRIPTION = "The Acetaldehyde (singlet) set of the QM-22 datasets, with energies calculated at the CCSD(T)/MRCI level of theory. QM-22 consists of CHON molecules of 4-15 atoms, developed in counterpoint to the MD17 dataset, run at higher total energies (above 500 K) and with a broader configuration space."  # noqa: E501

property_map = PropertyMap([energy_pd])
property_map.set_metadata_field("software", "MOLPRO")
property_map.set_metadata_field("method", "CCSD(T)/MRCI")
property_map.set_metadata_field("basis-set", "aug-cc-pVTZ/cc-pVTZ")
property_map.set_metadata_field("input", "")

energy_info = property_info(
    property_name="energy",
    field="energy",
    units="hartree",
    original_file_key="no key in file",
    additional=[("per-atom", {"value": False, "units": None})],
)
property_map.set_properties([energy_info])
PROPERTY_MAP = property_map.get_property_map()


def reader(filepath):
    symbols = []
    positions = []
    energy = None
    count = 0
    with open(filepath, "r") as f:
        for line in f:
            l_parts = line.strip().split()
            if len(l_parts) == 1 and l_parts[0].isdigit():
                natoms = int(l_parts[0])
            elif len(l_parts) == 1 and "." in l_parts[0]:
                energy = float(l_parts[0])
            if len(l_parts) == 4:
                s, x, y, z = l_parts
                symbols.append(s)
                positions.append([float(x), float(y), float(z)])
                if len(positions) == natoms:
                    yield AtomicConfiguration(
                        symbols=symbols,
                        positions=np.array(positions),
                        info={
                            "energy": energy,
                            "_name": f"{DATASET_NAME}_{count}",
                        },
                    )
                    count += 1
                    symbols = []
                    positions = []
                    energy = None


dm = DataManager(
    configs=reader(DATASET_FP),
    prop_defs=[
        energy_pd,
    ],
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
)
dm.load_co_po_to_vastdb(loader, batching_ingest=True)
dm.create_dataset(
    loader,
    name=DATASET_NAME,
    authors=AUTHORS,
    publication_link=PUBLICATION,
    other_links=OTHER_LINKS,
    data_link=DATA_LINK,
    data_license=LICENSE,
    description=DESCRIPTION,
    publication_year=PUBLICATION_YEAR,
)
print("complete")

"""
author: Gregory Wolfe

Properties
----------
energy
forces
stress


Other properties added to metadata
----------------------------------


File notes
----------
Due to unreliable install of fairchem (dependencies on torch, torch-geometric,
torch-scatter), I have preprocessed all files locally, where the install worked.
The data is stored in a pickle file of ASE atoms objects, which is read in here.

There is an associated metadata.npz file, but this contains only the number of
atoms in each structure

Converting first to ASE atoms object allows convenient retrieval of  props through
atoms.info. Not sure where some of this info is hiding in the "AseDBDataset" object

Take all properties from the atoms.calc object. There are some other energy values in info,
but it appears these were used as a comparison to the Materials Project 2020 values

 atoms.info['energy_corrected_mp2020'], which is connected to other
properties that will be included in the metadata of the POs. For example,
 'energy_correction_uncertainty_mp2020': 0.004,
 'energy_adjustments_mp2020': ['Composition-based energy adjustment (-0.687 eV/atom x 2.0 atoms)'], # noqa
 'correction_warnings': ['Failed to guess oxidation states for Entry None (Ba6Ga2NO). Assigning anion ... # noqa

From the documentation:  total energy (eV), forces (eV/A) and stress (eV/A^3).
Thank you for these units
"""

import os
from time import time

from colabfit.tools.database import DataManager, VastDataLoader
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID", -1))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID", -1)

spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
    .config("spark.executor.memoryOverhead", "600")
    .config("spark.driver.memory", "12g")
    .config("spark.ui.showConsoleProgress", "true")
    .config("spark.driver.maxResultSize", 0)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.rpc.message.maxSize", "2047")
    .getOrCreate()
)

loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_spark_session(spark_session)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_omat"
loader.prop_object_table = "ndb.colabfit.dev.po_omat"
loader.config_set_table = "ndb.colabfit.dev.cs_omat"
loader.dataset_table = "ndb.colabfit.dev.ds_omat"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_omat"


# loader.config_table = "ndb.colabfit.dev.co_wip"
# loader.prop_object_table = "ndb.colabfit.dev.po_wip"
# loader.config_set_table = "ndb.colabfit.dev.cs_wip"
# loader.dataset_table = "ndb.colabfit.dev.ds_wip"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_NAME = "OMat24_train_rattled_1000"
DATASET_ID = "DS_opcbnu5xs3pe_0"
DESCRIPTION = "The rattled-1000 training split of OMat24 (Open Materials 2024). OMat24 is a large-scale open dataset of density functional theory (DFT) calculations. The dataset is available in sub-datasets and subsampled sub-datasets based on the structure generation strategy used. There are two main splits in OMat24: train and validation, each divided into the aforementioned subsampling and sub-datasets."  # noqa


DOI = None
PUBLICATION_YEAR = "2024"
AUTHORS = [
    "Luis Barroso-Luque",
    "Muhammed Shuaibi",
    "Xiang Fu",
    "Brandon M. Wood",
    "Misko Dzamba",
    "Meng Gao",
    "Ammar Rizvi",
    "C. Lawrence Zitnick",
    "Zachary W. Ulissi",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.48550/arXiv.2410.12771"
DATA_LINK = "https://fair-chem.github.io/core/datasets/omat24.html"
OTHER_LINKS = [
    "https://doi.org/10.1002/adma.202210788",
    "https://huggingface.co/datasets/fairchem/OMAT24",
]


def main():
    t0 = time()
    dm = DataManager(
        dataset_id=DATASET_ID,
    )
    t = time()
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
        # labels=labels,
    )
    print(f"Time to create dataset: {time() - t}")
    print(f"Total time: {time() - t0}")
    print("complete")


if __name__ == "__main__":
    main()

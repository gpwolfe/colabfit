import logging
import os
import sys
from pathlib import Path
from pickle import load
from time import time

import numpy as np
import pyspark
from colabfit.tools.property_definitions import band_gap_pd, energy_above_hull_pd
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


# loader.metadata_dir = "test_md/MDtest"


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alexmp/alexmp_pickles"  # noqa
)
DATASET_NAME = "Alex_MP-20_train"
DESCRIPTION = "This dataset contains structures from the Alexandria (Schmidt et al. 2022) and MP-20 (Materials Project 2020) datasets. Data has been modified as follows: Exclude structures containing the elements Tc, Pm, or any element with atomic number 84 or higher. Relax structures with DFT using a PBE functional in order to have consistent energies. For the training set, remove any structure with more than 20 atoms inside the unit cell. For the training set, remove any structure with energy above the hull higher than 0.1 eV/atom."  # noqa
DOI = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Claudio Zeni",
    "Robert Pinsler",
    "Daniel Zügner",
    "Andrew Fowler",
    "Matthew Horton",
    "Xiang Fu",
    "Zilong Wang",
    "Aliaksandra Shysheya",
    "Jonathan Crabbé",
    "Shoko Ueda",
    "Roberto Sordillo",
    "Lixin Sun",
    "Jake Smith",
    "Bichlien Nguyen",
    "Hannes Schulz",
    "Sarah Lewis",
    "Chin-Wei Huang",
    "Ziheng Lu",
    "Yichi Zhou",
    "Han Yang",
    "Hongxia Hao",
    "Jielan Li",
    "Chunlei Yang",
    "Wenjie Li",
    "Ryota Tomioka",
    "Tian Xie",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.1038/s41586-025-08628-5"
DATA_LINK = "https://github.com/microsoft/mattergen"


property_map = PropertyMap([energy_above_hull_pd, band_gap_pd])
property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "PBE")
property_map.set_metadata_field(
    "input",
    "Please see original datasets Alexandria and MP-20 for configuration creation details.",  # noqa E501
)
for field in [
    "mp_task_id",
    "dft_mag_density",
    "alex_mp_split",
    "alex_mp_file_key",
    "space_group",
    "chemical_system",
    "dft_bulk_modulus",
    "hhi_score",
    "ml_bulk_modulus",
    "alex_mp_id",
]:
    property_map.set_metadata_field(field, field, dynamic=True)

energy_over_hull_info = property_info(
    property_name="energy-above-hull",
    field="energy_above_hull",
    units="eV",
    original_file_key="energy_above_hull",
    # at the moment, set to False to prevent prop being recalced as per molecule
    additional=[("per-atom", {"value": False, "units": None})],
)
band_gap_info = property_info(
    property_name="band-gap",
    field="dft_band_gap",
    units="eV",
    original_file_key="dft_band_gap",
    additional=[("type", {"value": "unknown", "units": None})],
)

property_map.set_properties([energy_over_hull_info, band_gap_info])

PROPERTY_MAP = property_map.get_property_map()

DSS = {
    "test": (
        f"{DATASET_NAME}_test",
        f"The test split of the dataset {DATASET_NAME}. {DESCRIPTION}",
        "DS_5rjlk0wubpsf_0",
    ),
    "train": (
        f"{DATASET_NAME}_train",
        f"The train split of the dataset {DATASET_NAME}. {DESCRIPTION}",
        "DS_uluw9723f2n4_0",
    ),
    "val": (
        f"{DATASET_NAME}_validation",
        f"The validation split of the dataset {DATASET_NAME}. {DESCRIPTION}",
        "DS_2dgg8tui3p9x_0",
    ),
}


def config_reader(pkl_fp):
    with open(pkl_fp, "rb") as f:
        for config in load(f):
            config.info["_name"] = (
                f"alex_mp_{config.info['alex_mp_split']}_{config.info['id'].replace('alex<', '').replace('>', '')}"  # noqa
            )
            config.info["alex_mp_id"] = config.info.pop("id")
            if np.isnan(config.info["dft_band_gap"]):
                config.info.pop("dft_band_gap")
            yield AtomicConfiguration.from_ase(config)


def read_wrapper(split):
    fps = sorted(list(DATASET_FP.rglob(f"*{split}*.pickle")), key=lambda x: x.stem)
    for fp in fps:
        yield from config_reader(fp)


def main(split):
    loader.config_table = f"ndb.colabfit.dev.co_alexmp_{split}"
    loader.prop_object_table = f"ndb.colabfit.dev.po_alexmp_{split}"
    loader.config_set_table = f"ndb.colabfit.dev.cs_alexmp_{split}"
    loader.dataset_table = f"ndb.colabfit.dev.ds_alexmp_{split}"
    loader.co_cs_map_table = f"ndb.colabfit.dev.cs_co_map_alexmp_{split}"

    logging.info(loader.config_table)
    logging.info(loader.config_set_table)
    logging.info(loader.dataset_table)
    logging.info(loader.prop_object_table)
    t0 = time()
    config_generator = read_wrapper(split)
    logging.info("Creating DataManager")
    ds_id = DSS[split][2]
    ds_name = DSS[split][0]
    ds_description = DSS[split][1]
    dm = DataManager(
        configs=config_generator,
        prop_defs=[
            energy_above_hull_pd,
            band_gap_pd,
        ],
        prop_map=PROPERTY_MAP,
        dataset_id=ds_id,
        standardize_energy=True,
        read_write_batch_size=10000,
    )
    logging.info(f"Time to prep: {time() - t0}")
    t1 = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=True)
    t2 = time()
    logging.info(f"Time to load: {t2 - t1}")
    dm.create_dataset(
        loader=loader,
        name=ds_name,
        authors=AUTHORS,
        description=ds_description,
        data_license=LICENSE,
        publication_link=PUBLICATION,
        publication_year=PUBLICATION_YEAR,
        data_link=DATA_LINK,
        equilibrium=False,
    )
    logging.info(f"Total time: {time() - t0}")
    logging.info("complete")


if __name__ == "__main__":
    split = sys.argv[1]
    main(split)

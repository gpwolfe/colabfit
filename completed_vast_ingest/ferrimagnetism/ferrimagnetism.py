"""
Requires deactivating an assertion in ase site package espresso-out reader for ibzkpts.
skip all /tmp/ files

according to ase documentation in ase.io.espresso.py:
In PW the forces are consistent with the "total energy"; that's why
its value must be assigned to free_energy.
"""

import os
from pathlib import Path

from ase.io import iread
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration

from colabfit.tools.vast.configuration_set import configuration_set_info
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, PropertyInfo
from pyspark.sql import SparkSession
from vastdb.session import Session

jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark = (
    SparkSession.builder.appName("Test")
    .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", "30000000ms")
    .getOrCreate()
)


with open("/scratch/gw2338/vast/data-lake-main/spark/scripts/.env") as f:
    envvars = dict(x.strip().split("=") for x in f.readlines())

access_key = envvars.get("SPARK_ID")
access_secret = envvars.get("SPARK_KEY")
endpoint = envvars.get("SPARK_ENDPOINT")
session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

loader = VastDataLoader(
    spark_session=spark,
    access_key=envvars.get("SPARK_ID"),
    access_secret=envvars.get("SPARK_KEY"),
    endpoint=envvars.get("SPARK_ENDPOINT"),
)

loader.config_table = "ndb.colabfit.dev.co_ferro"
loader.config_set_table = "ndb.colabfit.dev.cs_ferro"
loader.dataset_table = "ndb.colabfit.dev.ds_ferro"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_ferro"


DATASET_FP = Path("diffmag/diffmag/data")
DATASET_NAME = "Ferrimagnetism_induced_by_thermal_vibrations_in_oxygen-deficient_manganite_heterostructures"  # noqa: E501
DATASET_ID = "DS_vesysby9oheq_0"
DESCRIPTION = "Data from the paper 'Ferrimagnetism induced by thermal vibrations in oxygen-deficient manganite heterostructures'. Includes Quantum ESPRESSO calculations of SrCaMnO3 and SrMnO3, stoichiometric and defective cells."  # noqa: E501
PUBLICATION = "https://doi.org/10.1103/2266-h6bk"
DATA_LINK = "https://doi.org/10.24435/materialscloud:9q-vd"
OTHER_LINKS = ["https://doi.org/10.48550/arXiv.2405.04630"]
PUBLICATION_YEAR = "2025"

AUTHORS = [
    "Moloud Kaviani",
    "Chiara Ricca",
    "Ulrich Aschauer",
]
LICENSE = "CC-BY-4.0"
DOI = None
property_keys = []
property_map = PropertyMap([energy_pd, cauchy_stress_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "Quantum ESPRESSO")
property_map.set_metadata_field("method", "DFT-PBEsol+U")
property_map.set_metadata_field(
    "input",
    {
        "kinetic-energy-cut-off": "70 Ry",
        "augmented density-cut-off": "840 Ry",
        "Gaussian-smearing": "0.02 Ry",
        "atomic-forces-convergence": "5x10^-2 eV/angstrom",
        "energy-convergence": "1.4x10^-5 eV",
    },
)
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)

energy_info = PropertyInfo(
    property_name="energy",
    field="free_energy",
    units="eV",
    original_file_key="total energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = PropertyInfo(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="force",
    additional=None,
)
stress_info = PropertyInfo(
    property_name="cauchy-stress",
    field="cauchy-stress",
    units="eV/angstrom^3",
    original_file_key="total   stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)

property_map.set_properties([energy_info, force_info, stress_info])

PROPERTY_MAP = property_map.get_property_map()


def reader(dataset_fp):
    fps = Path(dataset_fp).rglob("*.out")
    fps = sorted(list(fps))
    for fp in fps:
        if fp.stem.startswith(".") or "/tmp/" in str(fp):
            continue
        print(fp)
        fpname = str(fp).replace(str(DATASET_FP), "").lstrip("/").replace("/", "__")
        try:
            for i, config in enumerate(iread(fp, ":", format="espresso-out")):
                config.info["_name"] = f"{DATASET_NAME}__{fpname}_{i}"
                try:
                    config.info["cauchy-stress"] = config.get_stress(voigt=False)
                except Exception as e:
                    config.info["cauchy-stress"] = None
                yield AtomicConfiguration.from_ase(config)
        except Exception as e:
            print(f"Error reading {fp}: {e}")
            continue


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


CSS = [
    configuration_set_info(
        co_name_match="SrMnO3",
        cs_name=f"{DATASET_NAME}__SrMnO3",
        cs_description=f"Configurations of SrMnO3 from {DATASET_NAME}",
        co_label_match=None,
        ordered=False,
    ),
    configuration_set_info(
        co_name_match="SrCaMnO3",
        cs_name=f"{DATASET_NAME}__SrCaMnO3",
        cs_description=f"Configurations of SrCaMnO3 from {DATASET_NAME}",
        co_label_match=None,
        ordered=False,
    ),
]


# print(dm.co_po_example_rows())

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

# dm.create_configuration_sets(loader, CSS)

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

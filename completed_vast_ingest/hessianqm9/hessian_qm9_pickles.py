import logging

from multiprocessing import Pool
from pathlib import Path
from pickle import dump
from colabfit.tools.property_definitions import energy_pd, atomic_forces_pd
from colabfit.tools.vast.configuration import AtomicConfiguration

from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv

from datasets import load_from_disk

load_dotenv()
logger = logging.getLogger(__name__)


# jars = os.getenv("VASTDB_CONNECTOR_JARS")
# spark = (
#     SparkSession.builder.appName("Test")
#     .config("spark.jars", os.environ["VASTDB_CONNECTOR_JARS"])
#     .config("spark.executor.heartbeatInterval", 10000000)
#     .config("spark.network.timeout", "30000000ms")
#     .getOrCreate()
# )

# access_key = os.getenv("SPARK_ID")
# access_secret = os.getenv("SPARK_KEY")
# endpoint = os.getenv("SPARK_ENDPOINT")
# session = Session(access=access_key, secret=access_secret, endpoint=endpoint)

# loader = VastDataLoader(
#     spark_session=spark,
#     access_key=access_key,
#     access_secret=access_secret,
#     endpoint=endpoint,
# )

# loader.metadata_dir = "test_md/MDtest"
# loader.config_table = "ndb.colabfit.dev.co_hessian_qm9"
# loader.config_set_table = "ndb.colabfit.dev.cs_hessian_qm9"
# loader.dataset_table = "ndb.colabfit.dev.ds_hessian_qm9"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_hessian_qm9"


DATASET_FP = Path("hessian_qm9_DatasetDict")
DATASET_NAME = "Hessian_QM9"
DATASET_ID = "DS_gk9tv5a9498z_0"
DESCRIPTION = "Hessian QM9 is the first database of equilibrium configurations and numerical Hessian matrices, consisting of 41,645 molecules from the QM9 dataset at the wB97x/6-31G* level. Molecular Hessians were calculated in vacuum, as well as in water, tetrahydrofuran, and toluene using an implicit solvation model."  # noqa: E501
PUBLICATION = "https://doi.org/10.1038/s41597-024-04361-2"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.26363959.v4"
OTHER_LINKS = None
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Nicholas J. Williams",
    "Lara Kabalan",
    "Ljiljana Stojanovic",
    "Viktor Zólyomi",
    "Edward O. Pyzer-Knapp",
]
LICENSE = "CC0"
DOI = None
property_keys = [
    "frequencies",
    "normal_modes",
    "hessian",
    "label",
]


property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "NWChem")
property_map.set_metadata_field("method", "DFT-wB97x")
property_map.set_metadata_field("basis_set", "6-31G*")
property_map.set_metadata_field("input", {"energy_cutoff": "1e-6 eV"})
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)


energy_info = property_info(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
atomic_forces_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
    additional=None,
)

property_map.set_properties([energy_info, atomic_forces_info])
PROPERTY_MAP = property_map.get_property_map()


def ds_reader(split):
    pickle_dir = Path(f"pickles/{split}")
    pickle_dir.mkdir(exist_ok=True, parents=True)
    ds = load_from_disk(DATASET_FP)
    batch = 0
    configs = []
    for i, row in enumerate(ds[split]):
        info = row
        pos = info.pop("positions")
        atomic_numbers = info.pop("atomic_numbers")
        info["_name"] = f"{DATASET_NAME}_{split}__index_{i}"
        configs.append(
            AtomicConfiguration(
                atomic_numbers=atomic_numbers, positions=pos, info=info, pbc=True
            )
        )
        if len(configs) == 5000:
            with (pickle_dir / f"{split}_{batch}.pickle").open("wb") as f:
                dump(configs, f)
            logger.info(f"{split} -- {batch}")
            batch += 1
            configs = []
    if configs:
        with (pickle_dir / f"{split}_{batch}.pickle").open("wb") as f:
            dump(configs, f)
        logger.info(f"{split} -- {batch}")
    logger.info(f"Finished {split}")


def to_pickle():
    pickle_dir = Path("pickles")
    pickle_dir.mkdir(exist_ok=True)
    splits = ["vacuum", "water", "thf", "toluene"]
    p = Pool(4)
    p.map(ds_reader, splits)
    logger.info("Complete")


if __name__ == "__main__":
    to_pickle()

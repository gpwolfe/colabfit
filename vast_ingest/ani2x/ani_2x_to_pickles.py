import logging
import os
from multiprocessing import Pool
from pathlib import Path
from pickle import dump
import h5py
from colabfit.tools.property_definitions import energy_pd
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.configuration_set import configuration_set_info
from colabfit.tools.vast.database import DataManager, VastDataLoader, batched
from colabfit.tools.vast.property import PropertyMap, property_info
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from vastdb.session import Session

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
# loader.config_table = "ndb.colabfit.dev.co_ani2x"
# loader.config_set_table = "ndb.colabfit.dev.cs_ani2x"
# loader.dataset_table = "ndb.colabfit.dev.ds_ani2x"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_ani2x"


DATASET_FP = Path("data/final_h5/ANI-2x-wB97X-def2TZVPP.h5")
DATASET_NAME = "ANI-2x-wB97X-def2TZVPP"
DATASET_ID = "DS_5bwpr2n9zxz9_0"
DESCRIPTION = "ANI-2x-wB97X-def2TZVPP is a portion of the ANI-2x dataset, which includes DFT-calculated energies for structures from 2 to 63 atoms in size containing H, C, N, O, S, F, and Cl. This portion of ANI-2x was calculated in ORCA at the wB97X level of theory using the def2TZVPP basis set. Configuration sets are divided by number of atoms per structure. Dipoles are recorded in the metadata."  # noqa: E501
PUBLICATION = "https://doi.org/10.1021/acs.jctc.0c00121"
DATA_LINK = "https://doi.org/10.5281/zenodo.10108942"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"


AUTHORS = [
    "Christian Devereux",
    "Justin S. Smith",
    "Kate K. Huddleston",
    "Kipton Barros",
    "Roman Zubatyuk",
    "Olexandr Isayev",
    "Adrian E. Roitberg",
]
LICENSE = "CC-BY-4.0"
DOI = None
property_keys = [
    "mbis_atomic_charges",
    "mbis_atomic_dipole_magnitudes",
    "mbis_atomic_volumes",
    "mbis_atomic_octupole_magnitudes",
    "mbis_atomic_quadrupole_magnitudes",
    "dipoles",
]
property_map = PropertyMap([energy_pd])

property_map.set_metadata_field("software", "ORCA 4.2.1")
property_map.set_metadata_field("method", "DFT-wB97X")
property_map.set_metadata_field("basis_set", "def2TZVPP")
property_map.set_metadata_field(
    "input", {"software_keywords": "! wb97x def2-tzvpp def2/j rijcosx engrad"}
)
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)

energy_info = property_info(
    property_name="energy",
    field="energies",
    units="hartree",
    original_file_key="energies",
    additional=[("per-atom", {"value": False, "units": None})],
)
property_map.set_properties([energy_info])
PROPERTY_MAP = property_map.get_property_map()


def ani_reader(args):
    fp, num_atoms = args
    logger.info(f"fp: {fp}, num_atoms: {num_atoms}")
    css = configuration_set_info(
        co_name_match=f"__natoms_{num_atoms}__",
        cs_name=f"ANI-2x-wB97X-def2TZVPP__num-atoms_{num_atoms}",
        cs_description=f"Configurations from {DATASET_NAME} with {num_atoms} atoms",  # noqa
        co_label_match=None,
        ordered=False,
    )
    with h5py.File(fp) as h5:
        properties = h5[str(num_atoms)]
        coordinates = properties["coordinates"]
        mbis_atomic_charges = properties["mbis_atomic_charges"]
        mbis_atomic_dipole_magnitudes = properties["mbis_atomic_dipole_magnitudes"]
        mbis_atomic_volumes = properties["mbis_atomic_volumes"]
        mbis_atomic_octupole_magnitudes = properties["mbis_atomic_octupole_magnitudes"]
        mbis_atomic_quadrupole_magnitudes = properties[
            "mbis_atomic_quadrupole_magnitudes"
        ]
        species = properties["species"]
        energies = properties["energies"]
        dipoles = properties["dipoles"]
        configs = []
        batch = 0
        for i, coord in enumerate(coordinates):
            info = {}
            info["energy"] = energies[i]
            info["mbis_atomic_charges"] = mbis_atomic_charges[i]
            info["mbis_atomic_dipole_magnitudes"] = mbis_atomic_dipole_magnitudes[i]
            info["mbis_atomic_volumes"] = mbis_atomic_volumes[i]
            info["mbis_atomic_octupole_magnitudes"] = mbis_atomic_octupole_magnitudes[i]
            info["mbis_atomic_quadrupole_magnitudes"] = (
                mbis_atomic_quadrupole_magnitudes[i]
            )
            info["dipoles"] = dipoles[i]
            info["_name"] = f"{DATASET_NAME}__natoms_{num_atoms}__ix_{i}"
            config = AtomicConfiguration(
                positions=coord,
                numbers=species[i],
                info=info,
            )
            configs.append(config)
            if len(configs) >= 50_000:
                with open(f"pickles/ani2x_{num_atoms}_{batch}.pickle", "wb") as f:
                    dump(configs, f)
                    batch += 1
                    configs = []
                logger.info(
                    f"{num_atoms} batch {batch} saved with {len(configs)} configs"
                )
        if configs:
            with open(f"pickles/ani2x_{num_atoms}_{batch}.pickle", "wb") as f:
                dump(configs, f)
            logger.info(f"{num_atoms} batch {batch} saved with {len(configs)} configs")
        logger.info(f"{num_atoms} complete")
        # logger.info(f"Configs in {num_atoms}: {len(configs)}")
        # yield config
        return css


CSS = []


# def read_wrapper(args):
#     fp, num_atoms = args
#     logger.info(f"num_atoms: {num_atoms}")
#     css = configuration_set_info(
#         co_name_match=f"__natoms_{num_atoms}__",
#         cs_name=f"ANI-2x-wB97X-def2TZVPP__num-atoms_{num_atoms}",
#         cs_description=f"Configurations from {DATASET_NAME} with {num_atoms} atoms",  # noqa
#         co_label_match=None,
#         ordered=False,
#     )
#     for i, batch in enumerate(batched(ani_reader(fp, num_atoms=num_atoms), 100_000)):
#         with open(f"pickles/ani2x_{num_atoms}_{i}.pickle", "wb") as f:
#             dump(batch, f)
#     # configs = ani_reader(fp, num_atoms=num_atoms)
#     # with open(f"pickles/ani2x_{num_atoms}.pickle", "wb") as f:
#     #     dump(configs, f)
#     logger.info(f"Finished {num_atoms}")
#     logger.info(f"CSS: {css}")
#     return css


def to_pickle(fp):
    pickle_dir = Path("pickles")
    pickle_dir.mkdir(exist_ok=True)
    with h5py.File(fp) as h5:
        keys = list(h5.keys())
        logger.info(f"keys: {keys}")
    p = Pool(16)
    results = p.map(ani_reader, [(fp, num_atoms) for num_atoms in keys])
    return results


# def read_wrapper(fp):
#     with h5py.File(fp) as h5:
#         keys = h5.keys()
#         logger.info(f"keys: {keys}")
#     for num_atoms in keys:
#         CSS.append(
#             configuration_set_info(
#                 co_name_match=f"__natoms_{num_atoms}__",
#                 cs_name=f"ANI-2x-wB97X-def2TZVPP__num-atoms_{num_atoms}",
#                 cs_description=f"Configurations from {DATASET_NAME} with {num_atoms} atoms",  # noqa
#                 co_label_match=None,
#                 ordered=False,
#             ),
#         )
#         yield from ani_reader(
#             fp,
#             num_atoms=int(num_atoms),
#         )


# dm = DataManager(
#     prop_defs=[energy_pd],
#     configs=read_wrapper(DATASET_FP),
#     prop_map=PROPERTY_MAP,
#     dataset_id=DATASET_ID,
#     standardize_energy=True,
# )

# dm.load_co_po_to_vastdb(loader, True)
if __name__ == "__main__":
    results = to_pickle(DATASET_FP)
    with open("pickles/ani2x_config_set_map.pickle", "wb") as f:
        dump(results, f)

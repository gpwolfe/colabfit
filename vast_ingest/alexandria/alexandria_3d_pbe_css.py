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



One structure has not been included due to malformed forces array
file: alex_go_aad_020.json
id: agm005676148
trajectory: 0
step index: 5
See structure below

{'structure': {'@module': 'pymatgen.core.structure',
    '@class': 'Structure',
    'charge': 0,
    'lattice': {'matrix': [[6.71735939, -0.03082698, -1.74953438],
      [-2.97750306, 5.80766944, -2.36152573],
      [-0.06458793, 0.82906572, 8.92165681]],
     'pbc': [True, True, True],
     'a': 6.941522745328944,
     'b': 6.940558520037945,
     'c': 8.960328219669197,
     'alpha': 104.9671891698746,
     'beta': 104.97144212134342,
     'gamma': 109.45743714003841,
     'volume': 364.0440827137655},
    'properties': {},
    'sites': [{'species': [{'element': 'Tb', 'occu': 1}],
      'abc': [0.31268224, 0.06268224, 0.62536448],
      'xyz': [1.873371422283853, 0.8728669333935359, 4.884213220254662],
      'properties': {},
      'label': 'Tb'},
     {'species': [{'element': 'Tb', 'occu': 1}],
      'abc': [0.56220587, 0.31220587, 0.12441173],
      'xyz': [2.8389094505742385, 1.8990028815861357, -0.6109219350664644],
      'properties': {},
      'label': 'Tb'},
     {'species': [{'element': 'Tb', 'occu': 1}],
      'abc': [0.68731776, 0.93731776, 0.37463552],
      'xyz': [1.8018969777161469, 5.733041246606464, -0.07361652025466228],
      'properties': {},
      'label': 'Tb'},
     {'species': [{'element': 'Tb', 'occu': 1}],
      'abc': [0.43779413, 0.68779413, 0.87558827],
      'xyz': [0.8363589494257618, 4.706905298413864, 5.421518635066464],
      'properties': {},
      'label': 'Tb'},
     {'species': [{'element': 'Ho', 'occu': 1}],
      'abc': [0.93737748, 0.18737748, 0.87475497],
      'xyz': [5.682285784407936, 1.784559306538429, 5.7217927666415225],
      'properties': {},
      'label': 'Ho'},
     {'species': [{'element': 'Ho', 'occu': 1}],
      'abc': [0.06262252, 0.81262252, 0.12524503],
      'xyz': [-2.0070173844079364, 4.82134887346157, -0.911196066641523],
      'properties': {},
      'label': 'Ho'},
     {'species': [{'element': 'Ho', 'occu': 1}],
      'abc': [0.18767281, 0.43767281, 0.37534562],
      'xyz': [-0.06674921518297933, 2.847259804089659, 1.9867891713540629],
      'properties': {},
      'label': 'Ho'},
     {'species': [{'element': 'Ho', 'occu': 1}],
      'abc': [0.81232719, 0.56232719, 0.62465438],
      'xyz': [3.7420176151829794, 3.7586483759103406, 2.8238075286459363],
      'properties': {},
      'label': 'Ho'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.69017748, 0.4383629, 0.87487271],
      'xyz': [3.2744370825516733, 3.249917703811867, 5.562619574065476],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.18469522, 0.93650981, 0.87487271],
      'xyz': [-1.6043028719922932, 6.1585727811686715, 5.27059142113058],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.30982252, 0.5616371, 0.12512729],
      'xyz': [0.4008313174483271, 3.3559904761881327, -0.7520228740654756],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.81530478, 0.06349019, 0.12512729],
      'xyz': [5.2795712719922925, 0.447335398831328, -0.4599947211305801],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.93835559, 0.68654678, 0.37493787],
      'xyz': [4.234860134460434, 4.269158219477401, 0.08208375002356083],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.56341872, 0.81160892, 0.62506213],
      'xyz': [1.3277465173723952, 5.214405409155522, 2.6742340405345],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.06164441, 0.31345322, 0.62506213],
      'xyz': [-0.5595917344604341, 2.3367499605225985, 4.728512949976439],
      'properties': {},
      'label': 'P'},
     {'species': [{'element': 'P', 'occu': 1}],
      'abc': [0.43658128, 0.18839108, 0.37493787],
      'xyz': [2.3475218826276056, 1.3915027708444772, 2.1363626594655],
      'properties': {},
      'label': 'P'}]},
   'energy': -105.92396102,
   'forces': [[-0.00021793, -0.00044876, -0.00082885],
    [0.00186276, 0.00383572, 0.00708449],
    [0.00021793, 0.00044876, 0.00082885],
    [-0.00186276, -0.00383572, -0.00708449],
    [-0.00286273, -0.00589483, -0.01088763],
    [0.00286273, 0.00589483, 0.01088763],
    [0.00265601, 0.00546915, 0.01010142],
    [-0.00265601, -0.00546915, -0.01010142],
    [0.00943903],
    [-0.00365626, -0.00466598, 0.00767191],
    [-0.00527149, -0.00799199, 0.00152884],
    [0.00365626, 0.00466598, -0.00767191],
    [0.00131429, -0.00015313, 0.0004018],
    [0.00105779, -0.0006813, -0.00057372],
    [-0.00131429, 0.00015313, -0.0004018],
    [-0.00105779, 0.0006813, 0.00057372]],
   'stress': [[-0.01798887, 0.00533262, 0.0280218],
    [0.00533257, 0.05793227, 0.02113897],
    [0.02802183, 0.02113896, 0.0807522]]},
"""

import os
from pathlib import Path
from pickle import load
from time import time

import pyspark
from colabfit.tools.configuration import AtomicConfiguration  # noqa
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property import PropertyMap, property_info
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from dotenv import load_dotenv
from pyspark.sql import SparkSession

print("pyspark version: ", pyspark.__version__)
# Set up data loader environment
load_dotenv()
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")
ACTUAL_INDEX = int(os.getenv("ACTUAL_INDEX"))

n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
spark_session = (
    SparkSession.builder.appName(f"colabfit_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
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

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/alexandria_3d_pbe_traj/alexandria_pickles"  # noqa
)
DATASET_NAME = "Alexandria_geometry_optimization_paths_PBE_3D"
DATASET_ID = "DS_s6gf4z2hcjqy_0"
DESCRIPTION = "The Alexandria Materials Database contains theoretical crystal structures in 1D, 2D and 3D discovered by machine learning approaches using DFT with PBE, PBEsol and SCAN methods. This dataset represents the geometry optimization paths for 3D crystal structures from Alexandria calculated using PBE methods."  # noqa

DOI = None
PUBLICATION_YEAR = "2024"
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


property_map = PropertyMap([atomic_forces_pd, energy_pd, cauchy_stress_pd])
property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "PBE")
property_map.set_metadata_field("input", "input", dynamic=True)

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
stress_info = property_info(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info])

PROPERTY_MAP = property_map.get_property_map()


def read_alexandria(fp: Path):
    with fp.open("rb") as f:
        for config in load(f):
            yield config


# cs_name_match = f"alexandria_3d__file_{fp.stem}__id_{id}__trajectory_{j}"
# cs_label_match = None
# cs_name = f"{DATASET_NAME}__{fp.stem}__{id}__{j}"
# cs_description = f"Alexandria 3D PBE trajectory number {j} for material {id} from file {fp.name}"  # noqa
# CSS[cs_name] = {}
# CSS[cs_name]["name"] = cs_name
# CSS[cs_name]["description"] = cs_description
# CSS[cs_name]["ids"] = []
# (cs_name_match, cs_label_match, cs_name, cs_description)]


def read_wrapper(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    all_json_paths = sorted(list(dir_path.rglob("*.pkl")), key=lambda x: x.stem)
    print(f"slurm task id: {SLURM_TASK_ID}")
    print(f"ACTUAL_INDEX: {ACTUAL_INDEX}")
    if ACTUAL_INDEX > len(all_json_paths):
        print("ACTUAL_INDEX is greater than the number of files")
        return
    path = all_json_paths[ACTUAL_INDEX]
    css_dir = Path("alexandria_css")
    css_dir.mkdir(exist_ok=True)
    css_path = css_dir / f"{path.stem}.csv"
    with css_path.open("a") as f:
        f.write("cs_name,config_id\n")
        print(f"Processing file: {path}")
        reader = read_alexandria(path)
        for config in reader:
            if config.info["_name"] != [
                "alexandria_3d__file_alex_go_aad_020__id_agm005676148__trajectory_0__frame_5"  # noqa
            ]:
                _, filename, mat_id, traj, ix = config.info["_name"][0].split("__")
                cs_name = f"{DATASET_NAME}__{filename}__{mat_id}__{traj}"
                f.write(f"{cs_name},{config.id}\n")
                yield config


def main():
    t0 = time()
    config_generator = read_wrapper(DATASET_FP)
    print("Creating DataManager")
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=10000,
    )
    print(f"Time to prep: {time() - t0}")
    t1 = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=True)
    t2 = time()
    print(f"Time to load: {t2 - t1}")
    print(f"Total time: {time() - t0}")
    print("complete")


if __name__ == "__main__":
    main()

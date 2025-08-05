import os
from pathlib import Path
from time import time

import numpy as np
import pandas as pd
import pyspark
from ase.io import iread
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.configuration_set import configuration_set_info
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyMap, property_info
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from pyspark.sql import SparkSession
from dotenv import load_dotenv

"""
From R. Lindsey:
    There are three possible sets of fields, corresponding to
    (1) the box dimensions,
    (2) the stress tensor, and
    (3) the system energy.
    Every file must contain box dimension fields,
    but stress and energy fields are options.
    One can include stresses and not energies or vice versa.

    The box dimension fields can take on two formats:
    box_len_x box_len_y box_len_z
    or
    NON_ORTHO  <9-size cell lattice vector>
    The stress tensor fields can take on two formats:
    <3-size diagonal components>
    or
    <6-size tensor components, as xx yy zz xy xz yz>
    Energy is always a single value.
Units:
energy: kcal/mol
forces: H/B
stress: GPa

stress is volume-normalized
"""
print("pyspark version: ", pyspark.__version__)
load_dotenv()
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID")

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
)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)


# loader.metadata_dir = "test_md/MDtest"
loader.config_table = "ndb.colabfit.dev.co_chimes"
loader.prop_object_table = "ndb.colabfit.dev.po_chimes"
loader.config_set_table = "ndb.colabfit.dev.cs_chimes"
loader.dataset_table = "ndb.colabfit.dev.ds_chimes"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_chimes"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
    sep="\n",
)


def chimes_stress_to_9_tensor(stress):
    """See order of stress arguments below."""
    if len(stress) == 6:
        xx, yy, zz, xy, xz, yz = stress
        return np.array([[xx, xy, xz], [xy, yy, yz], [xz, yz, zz]], dtype=float)
    elif len(stress) == 3:
        xx, yy, zz = stress
        return np.array([[xx, 0.0, 0.0], [0.0, yy, 0.0], [0.0, 0.0, zz]], dtype=float)
    else:
        raise ValueError(f"Unexpected length of stress: {len(stress)}: {stress}")


def get_pbc_from_cell(cell):
    cell = np.array(cell, dtype=float)
    cell_lengths = np.linalg.norm(cell, axis=1)
    pbc = cell_lengths > 1e-10
    return pbc.astype(bool).tolist()


def chimes_get_metadata_by_ix(index, md_map):
    """md_map keys should be tuples of (first_frame, last_frame)"""
    try:
        keys = list(md_map)
        key_bool = [x[0] <= index <= x[1] for x in keys]
        key = keys[key_bool.index(True)]
    except ValueError:
        return {
            "first_frame": "missing_index",
            "last_frame": "missing_index",
        }  # REMOVE BEFORE INGEST
    return md_map[key]


def chimes_md_map_from_file(fp):
    try:
        df = pd.read_csv(fp, header=0)
        cols = df.columns
        cols = [col.lower().replace(" ", "_") for col in cols]
        assert "first_frame" in cols and "last_frame" in cols
        df.columns = cols
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None
    md_map = {
        (row["first_frame"], row["last_frame"]): row.to_dict()
        for _, row in df.iterrows()
    }
    return md_map


def stress_to_9_tensor(stress):
    if len(stress) == 6:
        xx, yy, zz, xy, xz, yz = stress
        return np.array([[xx, xy, xz], [xy, yy, yz], [xz, yz, zz]], dtype=float)
    elif len(stress) == 3:
        xx, yy, zz = stress
        return np.array([[xx, 0.0, 0.0], [0.0, yy, 0.0], [0.0, 0.0, zz]], dtype=float)
    else:
        raise ValueError(f"Unexpected length of stress: {len(stress)}: {stress}")


def chimes_header_parser(headerline):
    """Parse ChIMES header, return cell, stress, and epot."""
    parts = [x.strip() for x in headerline.split()]
    parts_len = len(parts)
    stress = None
    epot = None
    if parts[0] == "NON_ORTHO":
        cell = np.array(parts[1:10], dtype=float).reshape(3, 3)
        if parts_len >= 16:
            stress = stress_to_9_tensor(np.array(parts[10:16], dtype=float))
            if parts_len == 17:
                epot = float(parts[-1])
        elif parts_len >= 13:
            stress = stress_to_9_tensor(np.array(parts[10:13], dtype=float))
            if parts_len == 14:
                epot = float(parts[-1])
        elif parts_len == 11:
            epot = float(parts[-1])
        else:
            raise ValueError(f"Unexpected length of parts: {parts_len}: {parts}")
    else:
        cell = np.diag(np.array([float(x) for x in parts[0:3]]))
        if 9 <= parts_len <= 10:
            stress = stress_to_9_tensor(np.array(parts[3:9], dtype=float))
            if parts_len == 10:
                epot = float(parts[-1])
        elif 6 <= parts_len <= 7:
            stress = stress_to_9_tensor(np.array(parts[3:6], dtype=float))
            if parts_len == 7:
                epot = float(parts[-1])
        elif parts_len == 4:
            epot = float(parts[-1])
        else:
            raise ValueError(f"Unexpected length of parts: {parts_len}: {parts}")
    pbc = get_pbc_from_cell(cell)
    return {
        "Lattice": cell,
        "stress": stress,
        "epot": epot,
        "pbc": pbc,
        "Properties": "species:S:1:pos:R:3:forces:R:3",
    }


def read_chimes(xyz, xls):
    for i, atom in enumerate(
        iread(xyz, format="extxyz", properties_parser=chimes_header_parser, index=":")
    ):
        if i > 600:
            break
        atom.info.update(chimes_get_metadata_by_ix(i, chimes_md_map_from_file(xls)))
        atom.info["_name"] = (
            f"{xyz.stem}__frame_range_{atom.info['first_frame']}-{atom.info['last_frame']}__index_{i}"
        )
        yield AtomicConfiguration.from_ase(atoms=atom, co_md_map=CO_MD_MAP)


DATASET_FP = Path(
    "/scratch/gw2338/colabfit-tools/data/ChIMES_C-2.0.Small_model_dataset.xyzf"
)
CHIMES_XLS_FP = Path(
    "/scratch/gw2338/colabfit-tools/data/ChIMES_C-2.0_Small_model_data_defs.csv"
)
DATASET_NAME = "ChIMES_C_2.0-Small_2025"
DATASET_ID = "DS_bkm2tahmkd0o_0"
DESCRIPTION = "The ChIMES C 2.0 Small dataset consists of initial structures of carbon calculated at the DFT level using VASP and trajectories produced using the ChIMES model. See links for the model code and ChIMES simulation evaluation library."  # noqa E501
PUBLICATION = "https://doi.org/10.1038/s41524-024-01497-y"
DATA_LINK = None
OTHER_LINKS = [
    "https://github.com/rk-lindsey/chimes_lsq",
]
PUBLICATION_YEAR = "2025"


AUTHORS = [
    "Rebecca K. Lindsey",
    "Nir Goldman",
    "Laurence E. Fried",
]
LICENSE = "CC-BY-4.0"
DOI = None
property_keys = [
    "cumul_frames",
    "labeling_method",
    "real_alc",
    "eff._alc",
    "case",
    "good/bad",
    "t_(k)",
    "rho_(gcc)",
    "class",
    "total_frames",
    "first_frame",
    "last_frame",
    "data_source",
    "path",
]
property_map = PropertyMap([energy_pd, cauchy_stress_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "ChIMES")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field("input", {"energy_cutoff": "1000 eV"})
for property in property_keys:
    property_map.set_metadata_field(property, property, dynamic=True)

energy_info = property_info(
    property_name="energy",
    field="epot",
    units="kcal/mol",
    original_file_key="in header",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = property_info(
    property_name="atomic-forces",
    field="forces",
    units="hartree/bohr",
    original_file_key="forces",
    additional=None,
)
stress_info = property_info(
    property_name="cauchy-stress",
    field="stress",
    units="GPa",
    original_file_key="in header",
    additional=[("volume-normalized", {"value": True, "units": None})],
)
property_map.set_properties([energy_info, force_info, stress_info])

PROPERTY_MAP = property_map.get_property_map()
CO_MD_MAP = {key: {"field": key} for key in ["phase", "t_(k)", "class"]}
chimes_gen = read_chimes(DATASET_FP, CHIMES_XLS_FP)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
    configs=chimes_gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
)

mdmap = chimes_md_map_from_file(CHIMES_XLS_FP)

cs_names = [
    f"{DATASET_NAME}__{row['phase']}_phase__temp_{row['t_(k)']}__frame_range_{row['first_frame']}-{row['last_frame']}"  # noqa E501
    for row in mdmap.values()
]
cs_name_match = [
    f"frame_range_{row['first_frame']}-{row['last_frame']}" for row in mdmap.values()
]
cs_descriptions = [
    f"Configurations from {DATASET_NAME}, {row['phase']} phase at {row['t_(k)']} K, of class \"{row['class']}\" frame range {row['first_frame']}-{row['last_frame']}"  # noqa E501
    for row in mdmap.values()
]
CSS = [
    (cs_name_match[i], None, cs_names[i], cs_descriptions[i], True)
    for i in range(len(cs_names))
]


dm.load_co_po_to_vastdb(loader)
dm.create_configuration_sets(loader, CSS)

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

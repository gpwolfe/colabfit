"""
author: Gregory Wolfe

Properties
----------
potential energy

Other properties added to metadata
----------------------------------
dipole, original scf energy without the energy correction subtracted

File notes
----------
File /scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/data/ \
    s2ef_train_20M/s2ef_train_20M/1050.extxyz is malformed, truncated

columns from txt files: system_id,frame_number,reference_energy

header from extxyz files:
Lattice=""
Properties=species:S:1:pos:R:3:move_mask:L:1:tags:I:1:forces:R:3
energy=-181.54722937
free_energy=-181.54878652
pbc="T T T"

get:
config.constraints
config.arrays (tags, forces)
config.info (energy, free_energy)

"""

import os
import pickle
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    energy_pd,
    adsorption_energy_pd,
)

print("pyspark version: ", pyspark.__version__)
# Set up data loader environment
load_dotenv()
SLURM_TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID", -1))
SLURM_JOB_ID = os.getenv("SLURM_JOB_ID", -1)
n_cpus = os.getenv("SLURM_CPUS_PER_TASK")
if not n_cpus:
    n_cpus = 1

spark_ui_port = os.getenv("__SPARK_UI_PORT")
jars = os.getenv("VASTDB_CONNECTOR_JARS")
print(jars)
spark_session = (
    SparkSession.builder.appName(f"cf_oc20_valid_{SLURM_JOB_ID}_{SLURM_TASK_ID}")
    .master(f"local[{n_cpus}]")
    .config("spark.ui.port", f"{spark_ui_port}")
    .config("spark.jars", jars)
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

# loader.metadata_dir = "test_md/MD"
# loader.config_table = "ndb.colabfit.dev.co_20m"
# loader.prop_object_table = "ndb.colabfit.dev.po_20m"
# loader.config_set_table = "ndb.colabfit.dev.cs_20m"
# loader.dataset_table = "ndb.colabfit.dev.ds_20m"
# loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_20m"

loader.config_table = "ndb.colabfit.dev.co_oc_reset"
loader.prop_object_table = "ndb.colabfit.dev.po_oc_reset3"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_oc_reset"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/s2ef_train_20M"
)
DATASET_NAME = "OC20_S2EF_train_20M"
DATASET_ID = "DS_otx1qc9f3pm4_0"
PUBLICATION_YEAR = "2024"
AUTHORS = [
    "Lowik Chanussot",
    "Abhishek Das",
    "Siddharth Goyal",
    "Thibaut Lavril",
    "Muhammed Shuaibi",
    "Morgane Riviere",
    "Kevin Tran",
    "Javier Heras-Domingo",
    "Caleb Ho",
    "Weihua Hu",
    "Aini Palizhati",
    "Anuroop Sriram",
    "Brandon Wood",
    "Junwoong Yoon",
    "Devi Parikh",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.1021/acscatal.0c04525"
DATA_LINK = "https://fair-chem.github.io/core/datasets/oc20.html"
DESCRIPTION = (
    "OC20_S2EF_train_20M is the 20 million structure training subset of the OC20 "
    "Structure to Energy and Forces dataset. Features include potential energy, "
    "free energy and atomic forces. Data from the OC20 mappings file, including "
    "adsorbate id, materials project bulk id, miller index, shift and others."
)


PKL_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/oc20_data_mapping.pkl"  # noqa
)
with open(PKL_FP, "rb") as f:
    OC20_MAP = pickle.load(f)

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-rPBE"},
    "basis_set": {"value": "def2-TZVPP"},
    "input": {
        "value": {
            "EDIFFG": "1E-3",
        },
    },
    "property_keys": {
        "value": {
            "energy": "free_energy",
            "atomic_forces": "forces",
            "adsorption_energy": "free_energy - reference_energy",
        }
    },
    "reference_energy_for_adsorption_energy": {
        "field": "reference_energy",
    },
}


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    adsorption_energy_pd["property-name"]: [
        {
            "energy": {"field": "adsorption_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        },
    ],
    "_metadata": PI_METADATA,
}


CO_METADATA = {
    key: {"field": key}
    for key in [
        "constraints",
        "bulk_id",
        "ads_id",
        "bulk_mpid",
        "bulk_symbols",
        "ads_symbols",
        "miller_index",
        "shift",
        "top",
        "adsorption_site",
        "class",
        "anomaly",
        "system_id",
        "frame_number",
    ]
}


def oc_reader(fp: Path):
    fp_num = f"{int(fp.stem):04d}"
    prop_fp = fp.with_suffix(".txt")
    with prop_fp.open("r") as prop_f:
        prop_lines = [x.strip() for x in prop_f.readlines()]
        iter_configs = iread(fp, format="extxyz", index=":")
        for i, config in enumerate(iter_configs):
            try:
                system_id, frame_number, reference_energy = prop_lines[i].split(",")
                reference_energy = float(reference_energy)
                config.info["constraints-fix-atoms"] = config.constraints[0].index
                config_data = OC20_MAP[system_id]
                config.info.update(config_data)
                config.info["reference_energy"] = reference_energy
                config.info["adsorption_energy"] = (
                    config.info["free_energy"] - reference_energy
                )
                config.info["system_id"] = system_id
                config.info["frame_number"] = frame_number.replace("frame", "")
                config.info["_name"] = f"{DATASET_NAME}__file_{fp_num}__config_{i}"
                config.info["_labels"] = [
                    f"frame:{frame_number}",
                    f"system:{system_id}",
                    f"materials_project_id:{config_data['bulk_mpid']}",
                    f"file:{fp.name}",
                ]
                yield AtomicConfiguration.from_ase(config, CO_METADATA)
            except Exception as e:
                print("Error encountered: ", e)
                continue


def oc_wrapper(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    xyz_paths = sorted(
        list(dir_path.rglob("*.extxyz")),
        key=lambda x: int(x.stem),
    )
    if SLURM_TASK_ID > len(xyz_paths):
        raise ValueError(f"This task ID {SLURM_TASK_ID} greater than number of files")
    xyz_path = xyz_paths[SLURM_TASK_ID]
    print(xyz_path.name)
    reader = oc_reader(xyz_path)
    for config in reader:
        yield config


skips = [
    2000,
    2001,
    2002,
    2003,
    2004,
    2005,
    2006,
    2007,
    2008,
    2009,
    2010,
    2011,
    2012,
    2013,
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
    2025,
    2026,
    2027,
    2028,
    2029,
    2030,
    2031,
    2032,
    2033,
    2034,
    2035,
    2036,
    2037,
    2038,
    2039,
    2040,
    2041,
    2042,
    2043,
    2044,
    2045,
    2046,
    2047,
    2048,
    2049,
    2050,
    2051,
    2052,
    2053,
    2054,
    2055,
    2056,
    2057,
    2058,
    2059,
    2060,
    2061,
    2062,
    2063,
    2064,
    2065,
    2066,
    2067,
    2068,
    2069,
    2070,
    2071,
    2072,
    2073,
    2074,
    2075,
    2076,
    2077,
    2078,
    2079,
    2080,
    2081,
    2082,
    2083,
    2084,
    2085,
    2086,
    2087,
    2088,
    2089,
    2090,
    2091,
    2092,
    2093,
    2094,
    2095,
    2096,
    2097,
    2098,
    2099,
    2100,
    2101,
    2102,
    2103,
    2104,
    2105,
    2106,
    2107,
    2108,
    2109,
    2110,
    2111,
    2112,
    2113,
    2114,
    2115,
    2116,
    2117,
    2118,
    2119,
    2120,
    2121,
    2122,
    2123,
    2124,
    2125,
    2126,
    2127,
    2128,
    2129,
    2130,
    2131,
    2132,
    2133,
    2134,
    2135,
    2136,
    2137,
    2138,
    2139,
    2140,
    2141,
    2142,
    2143,
    2144,
    2145,
    2146,
    2147,
    2148,
    2149,
    2150,
    2151,
    2152,
    2153,
    2154,
    2155,
    2156,
    2157,
    2158,
    2159,
    2160,
    2161,
    2162,
    2163,
    2164,
    2165,
    2166,
    2167,
    2168,
    2169,
    2170,
    2171,
    2172,
    2173,
    2174,
    2175,
    2176,
    2177,
    2178,
    2179,
    2180,
    2181,
    2182,
    2183,
    2184,
    2185,
    2186,
    2188,
    2192,
    2193,
    2195,
    2196,
    2197,
    2198,
    2199,
    2207,
    2208,
    2210,
    2211,
    2212,
    2213,
    2215,
    2216,
    2218,
    2219,
    2221,
    2222,
    2224,
    2225,
    2227,
    2229,
    2232,
    2234,
    2236,
    2237,
    2239,
    2245,
    2291,
    2298,
    2312,
    2347,
    2396,
    2527,
    2528,
    2529,
    2531,
    2540,
    2576,
    2580,
    2592,
    2593,
    2596,
    2597,
    2630,
    2658,
    2672,
    2693,
    2694,
    2699,
    2719,
    2720,
    2731,
    2738,
    2751,
    2769,
    2796,
    2837,
    2844,
    2845,
    2848,
    2851,
    2853,
    2854,
    2871,
    2872,
    2885,
    2893,
    2909,
    2916,
    2917,
    2947,
    2959,
    2972,
    2982,
    2983,
]


def main():
    if int(SLURM_TASK_ID) in skips or SLURM_TASK_ID in skips:
        raise ValueError(f"Skipping task {SLURM_TASK_ID}")
    else:
        beg = time()
        config_generator = oc_wrapper(DATASET_FP)
        print("Creating DataManager")
        dm = DataManager(
            nprocs=1,
            configs=config_generator,
            prop_defs=[energy_pd, atomic_forces_pd, adsorption_energy_pd],
            prop_map=PROPERTY_MAP,
            dataset_id=DATASET_ID,
            standardize_energy=True,
            read_write_batch_size=10000,
        )
        print(f"Time to prep: {time() - beg}")
        t = time()
        dm.load_co_po_to_vastdb(loader, batching_ingest=True)
        print(f"Time to load: {time() - t}")
        print("complete")
        # labels = ["OC20", "Open Catalyst"]
        # print("Creating dataset")
        # t = time()
        # dm.create_dataset(
        #     loader,
        #     name=DATASET_NAME,
        #     authors=AUTHORS,
        #     publication_link=PUBLICATION,
        #     data_link=DATA_LINK,
        #     data_license=LICENSE,
        #     description=DESCRIPTION,
        #     publication_year=PUBLICATION_YEAR,
        #     labels=labels,
        # )
        # print(f"Time to create dataset: {time() - t}")
        # loader.stop_spark()
        # print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

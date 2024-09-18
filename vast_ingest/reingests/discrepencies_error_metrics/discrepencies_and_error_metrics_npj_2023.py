"""
author: Gregory Wolfe

Properties
----------
forces (up to 3 k-point settings)
energy (up to 2 k-point settingss)

Other properties added to metadata
----------------------------------
vacancy
distance to vacancy
distance to migrating atoms


File notes
----------
Can be run from ingest pod

vacancy position where exists added to CO metadata as dict (ase.Atoms.todict)

Enhanced_Validation_Set DATA.json file:
keys = ['Energies', 'Forces', 'r', 'r_v']
Energies keys = 'DFT_K4'
Forces keys = 'DFT_K4'

Interstitial-enhanced_Training_Set
keys = ['Energies', 'Forces']
Energies keys = 'DFT_K4'
Forces keys = 'DFT_K4'

Interstitial-RE_Testing_Set
['Energies', 'Forces', 'r', 'r_v']
Energies keys = 'DFT_K4', 'DFT_K2'
Forces keys = 'DFT_K4', 'DFT_K2', 'DFT_K1'

Vacancy-enhanced_Training_Set
keys = ['Energies', 'Forces']
Energies keys = 'DFT_K4'
Forces keys = 'DFT_K4'

Vacancy-RE_Testing_Set
['Energies', 'Forces', 'r', 'r_v']
Energies keys = 'DFT_K4', 'DFT_K2'
Forces keys = 'DFT_K4', 'DFT_K2', 'DFT_K1'


In order to quantify the errors on predicted energies and atomic forces of these
MLIPs, we construct interstitial-RE and vacancy-RE testing sets, RE-ITesting and
RE-VTesting, respectively, each consisting of 100 snapshots of atomic configurations
with a single migrating vacancy or interstitial, respectively, from AIMD simulations
at 1230 K (Fig. 1a, b, Methods), with the true values of energies and atomic forces
evaluated by DFT calculations with a k-point mesh of 4x4x4 (DFT K4)
All DFT calculations are performed using VASP version 5.4.4

Notes on reingest:
These are the two datasets that should be reingested
# discrepencies_and_error_metrics_NPJ_2023_interstitial_re_testing_set no pos
# discrepencies_and_error_metrics_NPJ_2023_vacancy_re_testing_set

"""

import json
import os
from pathlib import Path
from time import time

from ase.io import read
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    SparkDataLoader,
)

from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd

# Set up data loader environment
load_dotenv()
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")


loader = SparkDataLoader(
    table_prefix="ndb.colabfit.dev",
)
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

# Define which tables will be used

# loader.config_table = "ndb.colabfit.dev.co_discrep"
# loader.config_set_table = "ndb.colabfit.dev.cs_discrep"
# loader.dataset_table = "ndb.colabfit.dev.ds_discrep"
# loader.prop_object_table = "ndb.colabfit.dev.po_discrep"

loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage4"
loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids_stage2"

print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/Silicon_MLIP_datasets-main"  # noqa
)
DATASET_NAME = "discrepencies_and_error_metrics_NPJ_2023"

LICENSE = "CC-BY-4.0"
PUBLICATION_YEAR = "2023"

PUBLICATION = "https://doi.org/10.1038/s41524-023-01123-3"
DATA_LINK = "https://github.com/mogroupumd/Silicon_MLIP_datasets"
LINKS = [
    "https://github.com/mogroupumd/Silicon_MLIP_datasets",
    "https://doi.org/10.1038/s41524-023-01123-3",
]
AUTHORS = ["Yunsheng Liu", "Xingfeng He", "Yifei Mo"]
DATASET_DESC = (
    f"The full {DATASET_NAME} dataset includes the original mlearn_Si_train dataset, "
    "modified with the purpose of developing models with better diffusivity scores "
    "by replacing ~54% of the data with structures containing migrating interstitials. "
    "The enhanced validation set contains 50 total structures, consisting of 20 "
    "structures randomly selected from the 120 replaced structures of the original "
    "training dataset, 11 snapshots with vacancy rare events (RE) from AIMD "
    "simulations, and 19 snapshots with interstitial RE from AIMD simulations. We also "
    "construct interstitial-RE and vacancy-RE testing sets, each consisting of 100 "
    "snapshots of atomic configurations with a single migrating vacancy or "
    "interstitial, respectively, from AIMD simulations at 1230 K."
)

GLOB_STR = "0.POSCAR.vasp"

PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        },
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        },
    ],
    "_metadata": {
        "software": {"value": "VASP 5.4.4"},
        "method": {"value": "DFT-PBE"},
        "input": {"field": "input"},
    },
}

CO_METADATA = {
    "distances-to-migrating-atoms": {"field": "r"},
    "distances-to-vacancy": {"field": "r_v"},
    "vacancy-location": {"field": "vacancy"},
}
# file_keys = [
#     "Enhanced_Validation_Set",
#     "Interstitial-enhanced_Training_Set",
#     "Interstitial-RE_Testing_Set",
#     "Vacancy-enhanced_Training_Set",
#     "Vacancy-RE_Testing_Set",
# ]
DSS = [
    # (
    #     f"{DATASET_NAME}_enhanced_validation_set",
    #     DATASET_FP / "Enhanced_Validation_Set",
    #     f"Structures from {DATASET_NAME} validation set, "
    #     "enhanced by inclusion of rare events. " + DATASET_DESC,
    # ),
    # (
    #     f"{DATASET_NAME}_interstitial_enhanced_training_set",
    #     DATASET_FP / "Interstitial-enhanced_Training_Set",
    #     f"Structures from {DATASET_NAME} training set, "
    #     "enhanced by inclusion of interstitials. " + DATASET_DESC,
    # ),
    (
        f"{DATASET_NAME}_interstitial_re_testing_set",
        DATASET_FP / "Interstitial-RE_Testing_Set",
        f"Structures from {DATASET_NAME} test set; these include an interstitial. "
        + DATASET_DESC,
        "10.60732/81a7ca9e",
        "DS_dhe9aqs9q1wf_0",
    ),
    # (
    #     f"{DATASET_NAME}_vacancy_enhanced_training_set",
    #     DATASET_FP / "Vacancy-enhanced_Training_Set",
    #     f"Structures from {DATASET_NAME} training set; includes "
    #     "some structures with vacancies. " + DATASET_DESC,
    # ),
    (
        f"{DATASET_NAME}_vacancy_re_testing_set",
        DATASET_FP / "Vacancy-RE_Testing_Set",
        f"Structures from {DATASET_NAME} test set; these include a single migrating vacancy. "  # noqa
        + DATASET_DESC,
        "10.60732/63c3da57",
        "DS_la08goe2lz0g_0",
    ),
]


def namer(file):
    return "__".join(
        [x for x in str(file).replace(str(DATASET_FP), "").split("/") if x != ""]
    )


def structure_reader(data_dir):
    struct_dir = data_dir / "structure_poscars"
    files = sorted(
        struct_dir.glob("*.POSCAR.vasp"), key=lambda x: int(x.stem.split(".")[0])
    )
    for file in files:
        yield read(file)


def vacancy_reader(data_dir):
    vacancies = dict()
    vac_dir = data_dir / "vacancy_poscars"
    files = sorted(
        list(vac_dir.glob("*vacancies.POSCAR.vasp")),
        key=lambda x: int(x.stem.split("_")[0]),
    )
    for file in files:
        # Only some configs have vacancies, indicated by first int in file stem
        vacancies[int(file.stem.split("_")[0])] = read(file).todict()
    return vacancies


def props_reader(data_dir):
    with open(data_dir / "DATA.json") as f:
        props = json.load(f)
        return props


def reader(fp):
    props = props_reader(fp)
    energies_k2 = props["Energies"].get("DFT_K2")
    energies_k4 = props["Energies"].get("DFT_K4")
    forces_k1 = props["Forces"].get("DFT_K1")
    forces_k2 = props["Forces"].get("DFT_K2")
    forces_k4 = props["Forces"].get("DFT_K4")
    # dft_dict = {
    # "DFT_K2": {
    #     "energy": energies_k2,
    #     "forces": forces_k2,
    #     "keys": {"energy": "Energies.DFT_K2", "atomic_forces": "Forces.DFT_K2"},
    #     "input": {
    #         "value": {
    #             "kpoints": {"value": "4x4x4"},
    #             "encut": {"value": 520},
    #         }
    #     },
    # },
    # "DFT_K4": {
    #     "energy": energies_k4,
    #     "forces": forces_k4,
    #     "keys": {"energy": "Energies.DFT_K4", "atomic_forces": "Forces.DFT_K4"},
    #     "input": {
    #         "value": {
    #             "kpoints": {"value": "2x2x2"},
    #             "encut": {"value": 520},
    #         }
    #     },
    # },
    #     "DFT_K1": {
    #         "forces": forces_k1,
    #         "keys": {"atomic_forces": "Forces.DFT_K1"},
    #         "input": {
    #             "value": {
    #                 "kpoints": {"value": "single gamma-point"},
    #                 "encut": {"value": 520},
    #             }
    #         },
    #     },
    # }
    r = props.get("r")
    r_v = props.get("r_v")
    structures = structure_reader(fp)
    vacancies = vacancy_reader(fp)
    for i, config in enumerate(structures):
        configk1 = config.copy()
        configk1.info["forces"] = forces_k1[i]
        configk1.info["keys"] = {"atomic_forces": "Forces.DFT_K1"}
        configk1.info["input"] = (
            {
                "value": {
                    "kpoints": {"value": "single gamma-point"},
                    "encut": {"value": 520},
                }
            },
        )
        configk1.info["_name"] = namer(fp) + f"__DFT_K1__{i}"
        if r is not None:
            configk1.info["r"] = r[i]
        if r_v is not None:
            configk1.info["r_v"] = r_v[i]
        if vacancies.get(i) is not None:
            configk1.info["vacancy"] = {
                key: val.tolist() for key, val in vacancies[i].items()
            }
        yield AtomicConfiguration.from_ase(configk1, co_md_map=CO_METADATA)
        configk2 = config.copy()
        configk2.info["forces"] = forces_k2[i]
        configk2.info["energy"] = energies_k2[i]
        configk2.info["keys"] = {
            "energy": "Energies.DFT_K2",
            "atomic_forces": "Forces.DFT_K2",
        }
        configk2.info["input"] = {
            "value": {
                "kpoints": {"value": "4x4x4"},
                "encut": {"value": 520},
            }
        }
        configk2.info["_name"] = namer(fp) + "__DFT_K2"
        if r is not None:
            configk2.info["r"] = r[i]
        if r_v is not None:
            configk2.info["r_v"] = r_v[i]
        if vacancies.get(i) is not None:
            configk2.info["vacancy"] = {
                key: val.tolist() for key, val in vacancies[i].items()
            }
        yield AtomicConfiguration.from_ase(configk2, co_md_map=CO_METADATA)
        configk4 = config.copy()
        configk4.info["forces"] = forces_k4[i]
        configk4.info["energy"] = energies_k4[i]
        configk4.info["keys"] = {
            "energy": "Energies.DFT_K4",
            "atomic_forces": "Forces.DFT_K4",
        }
        configk4.info["input"] = {
            "value": {
                "kpoints": {"value": "2x2x2"},
                "encut": {"value": 520},
            }
        }
        configk4.info["_name"] = namer(fp) + "__DFT_K4"
        if r is not None:
            configk4.info["r"] = r[i]
        if r_v is not None:
            configk4.info["r_v"] = r_v[i]
        if vacancies.get(i) is not None:
            configk4.info["vacancy"] = {
                key: val.tolist() for key, val in vacancies[i].items()
            }
        yield AtomicConfiguration.from_ase(configk4, co_md_map=CO_METADATA)
        # for key, val in dft_dict.items():
        #     if val.get("energy") is not None:
        #         energies = val["energy"]
        #         config.info["energy"] = energies[i]
        #     config.info["forces"] = val["forces"][i]
        #     config.info["keys"] = val["keys"]
        # config.info["_name"] = namer(fp) + f"__{key}"
        # config.info["input"] = val["input"]
        # if r is not None:
        #     config.info["r"] = r[i]
        # if r_v is not None:
        #     config.info["r_v"] = r_v[i]
        # if vacancies.get(i) is not None:
        #     config.info["vacancy"] = {
        #         key: val.tolist() for key, val in vacancies[i].items()
        #     }
        # yield AtomicConfiguration.from_ase(config, co_md_map=CO_METADATA)


def main():
    for name, data_dir, description, doi, dataset_id in DSS:
        beg = time()
        # loader.zero_multiplicity(ds_id)
        config_generator = reader(data_dir)
        dm = DataManager(
            nprocs=1,
            configs=config_generator,
            prop_defs=[energy_pd, atomic_forces_pd],
            prop_map=PROPERTY_MAP,
            dataset_id=dataset_id,
            standardize_energy=True,
            read_write_batch_size=100000,
        )
        print(f"Time to prep: {time() - beg}")
        t = time()
        dm.load_co_po_to_vastdb(loader, batching_ingest=False)
        print(f"Time to load: {time() - t}")
        print("Creating dataset")
        t = time()
        dm.create_dataset(
            loader,
            name=name,
            authors=AUTHORS,
            publication_link=PUBLICATION,
            data_link=DATA_LINK,
            data_license=LICENSE,
            description=description,
            publication_year=PUBLICATION_YEAR,
            doi=doi,
        )
        print(f"Time to create dataset: {time() - t}")

        print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

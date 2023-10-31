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
"""
from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/Silicon_MLIP_datasets-main_discrepencies_and_error_metrics_npj2023"
)
DATASET_NAME = "discrepencies_and_error_metrics_NPJ_2023"

SOFTWARE = "VASP 5.4.4"
METHODS = "DFT-PBE-GGA"

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
ELEMENTS = None
GLOB_STR = "0.POSCAR.vasp"
PI_METADATA_K1 = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "kpoint": {"value": "single gamma-point"},
    "energy-cutoff": {"value": "520 eV"},
}
PI_METADATA_K2 = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "kpoint": {"value": "2x2x2"},
    "energy-cutoff": {"value": "520 eV"},
}
PI_METADATA_K4 = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "kpoint": {"value": "4x4x4"},
    "energy-cutoff": {"value": "520 eV"},
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy_k2", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA_K2,
        },
        {
            "energy": {"field": "energy_k4", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA_K4,
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces_k1", "units": "eV/A"},
            "_metadata": PI_METADATA_K1,
        },
        {
            "forces": {"field": "forces_k2", "units": "eV/A"},
            "_metadata": PI_METADATA_K2,
        },
        {
            "forces": {"field": "forces_k4", "units": "eV/A"},
            "_metadata": PI_METADATA_K4,
        },
    ],
}

CO_METADATA = {
    "distances-to-migrating-atoms": {"field": "r", "units": "Ha"},
    "distances-to-vacancy": {"field": "r_v", "units": "Ha"},
    "vacancy-location": {"field": "vacancy"},
}
file_keys = [
    "Enhanced_Validation_Set",
    "Interstitial-enhanced_Training_Set",
    "Interstitial-RE_Testing_Set",
    "Vacancy-enhanced_Training_Set",
    "Vacancy-RE_Testing_Set",
]
DSS = [
    (
        f"{DATASET_NAME}_enhanced_validation_set",
        DATASET_FP / "Enhanced_Validation_Set",
        f"Structures from {DATASET_NAME} validation set, "
        "enhanced by inclusion of rare events. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_interstitial_enhanced_training_set",
        DATASET_FP / "Interstitial-enhanced_Training_Set",
        f"Structures from {DATASET_NAME} training set, "
        "enhanced by inclusion of interstitials. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_interstitial_re_testing_set",
        DATASET_FP / "Interstitial-RE_Testing_Set",
        f"Structures from {DATASET_NAME} test set; these include "
        "an interstitial. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_vacancy_enhanced_training_set",
        DATASET_FP / "Vacancy-enhanced_Training_Set",
        f"Structures from {DATASET_NAME} training set; includes "
        "some structures with vacancies. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_vacancy_re_testing_set",
        DATASET_FP / "Vacancy-RE_Testing_Set",
        f"Structures from {DATASET_NAME} test set; these "
        "include a single migrating vacancy. " + DATASET_DESC,
    ),
]


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
    data_dir = fp.parents[1]
    props = props_reader(data_dir)
    energies_k2 = props["Energies"].get("DFT_K2")
    energies_k4 = props["Energies"].get("DFT_K4")
    forces_k1 = props["Forces"].get("DFT_K1")
    forces_k2 = props["Forces"].get("DFT_K2")
    forces_k4 = props["Forces"].get("DFT_K4")
    r = props.get("r")
    r_v = props.get("r_v")
    structures = structure_reader(data_dir)
    vacancies = vacancy_reader(data_dir)
    if forces_k1 is not None:
        info_list = [
            {
                "energy_k4": energies_k4[i],
                "forces_k4": forces_k4[i],
                "forces_k1": forces_k1[i],
                "forces_k2": forces_k2[i],
                "energy_k2": energies_k2[i],
                "r": r[i],
                "r_v": r_v[i],
            }
            for i, val in enumerate(forces_k4)
        ]
    else:
        info_list = [
            {"forces_k4": forces_k4[i], "energy_k4": energies_k4[i]}
            for i, val in enumerate(forces_k4)
        ]
    for i, info_dict in enumerate(info_list):
        config = next(structures)
        config.info = info_dict
        if vacancies.get(i) is not None:
            config.info["vacancy"] = {
                key: val.tolist() for key, val in vacancies[i].items()
            }
        config.info["name"] = f"{data_dir.parts[-1]}_{i}"
        yield config


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    for name, data_dir, desc in DSS:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=data_dir,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=GLOB_STR,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        # cs_ids = []
        # for i, (name, query, desc) in enumerate(CSS):
        #     cs_id = client.query_and_insert_configuration_set(
        #         co_hashes=all_co_ids,
        #         ds_id=ds_id,
        #         name=name,
        #         description=desc,
        #         query=query,
        #     )

        #     cs_ids.append(cs_id)

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=LINKS,
            description=desc,
            verbose=True,
            # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        )


if __name__ == "__main__":
    main(sys.argv[1:])

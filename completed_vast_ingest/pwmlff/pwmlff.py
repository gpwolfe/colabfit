"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
about 17K configurations
There are more data files that might be available, but require downloading and running
a "netdisk" client in order to download that larger directories of data.
"""

import os
from pathlib import Path
import re
from collections import Counter


# from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, VastDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)

load_dotenv()
loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

# loader.metadata_dir = (
#     "test_md/MD"  # comment out upon full ingest  # comment out upon full ingest
# )

# Define which tables will be used

# loader.config_table = "ndb.colabfit.dev.co_pwm"
# loader.prop_object_table = "ndb.colabfit.dev.po_pwm"
# loader.config_set_table = "ndb.colabfit.dev.cs_pwm"
# loader.dataset_table = "ndb.colabfit.dev.ds_pwm"
# loader.co_cs_map_table = "ndb.colabfit.dev.co_cs_map_pwm"

loader.config_table = "ndb.colabfit.dev.co_wip"
loader.prop_object_table = "ndb.colabfit.dev.po_wip"
loader.config_set_table = "ndb.colabfit.dev.cs_wip"
loader.dataset_table = "ndb.colabfit.dev.ds_wip"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_wip"


DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/pwmlff/available_data"  # noqa E501
)

DATASET_NAME = "PWMLFF_feature_comparison_NPJ2023"
AUTHORS = ["Ting Han", "Jie Li", "Liping Liu", "Fengyu Li", "Lin-Wang Wang"]
PUBLICATION_LINK = "https://www.doi.org/10.1088/1367-2630/acf2bb"
DATA_LINK = "https://github.com/LonxunQuantum/PWMLFF_library/tree/main"
OTHER_LINKS = None
DS_DESCRIPTION = 'Partial dataset for "Accuracy evaluation of different machine learning force field features". The included data is limited to that hosted directly on the repository at the related GitHub link. From publication abstract: Predicting energies and forces using machine learning force field (MLFF) depends on accurate descriptions (features) of chemical environment. Despite the numerous features proposed, there is a lack of controlled comparison among them for their universality and accuracy. In this work, we compared several commonly used feature types for their ability to describe physical systems. These different feature types include cosine feature, Gaussian feature, moment tensor potential (MTP) feature, spectral neighbor analysis potential feature, simplified smooth deep potential with Chebyshev polynomials feature and Gaussian polynomials feature, and atomic cluster expansion feature. We evaluated the training root mean square error (RMSE) for the atomic group energy, total energy, and force using linear regression model regarding to the density functional theory results. We applied these MLFF models to an amorphous sulfur system and carbon systems, and the fitting results show that MTP feature can yield the smallest RMSE results compared with other feature types for either sulfur system or carbon system in the disordered atomic configurations. Moreover, as an extending test of other systems, the MTP feature combined with linear regression model can also reproduce similar quantities along the ab initio molecular dynamics trajectory as represented by Cu systems. Our results are helpful in selecting the proper features for the MLFF development.'  # noqa E501
DS_LABELS = None  # ["label1", "label2"]
LICENSE = "CC-BY-4.0"
GLOB_STR = "MOVEMENT"

ds_id = "DS_cgjdk1e2txjy_0"

PI_METADATA = {
    "software": {"value": "PWmat"},
    "method": {"value": "DFT-PBE"},
    "input": {"field": "input"},
    "property_keys": {
        "value": {
            "energy": "eTot",
            "forces": "Force",
            "stress": "Pressure Internal",
        }
    },
}

PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        },
    ],
    cauchy_stress_pd["property-name"]: [
        {
            "stress": {"field": "stress", "units": "hartree/bohr^3"},
            "volume-normalized": {"value": False, "units": None},
        }
    ],
    "_metadata": PI_METADATA,
}


CONFIGURATION_SETS = [
    (
        r"C__",
        None,
        f"{DATASET_NAME}__carbon",
        f"Structures of carbon from {DATASET_NAME}",
    ),
    (
        r"CH3CH2OH__",
        None,
        f"{DATASET_NAME}__CH3CH2OH",
        f"Structures of CH3CH2OH from {DATASET_NAME}",
    ),
    (
        r"CH4__",
        None,
        f"{DATASET_NAME}__CH4",
        f"Structures of CH4 from {DATASET_NAME}",
    ),
    # (
    #     r"LiGePS__.*",
    #     None,
    #     f"{DATASET_NAME}__LiGePS",
    #     f"Structures of LiGePS from {DATASET_NAME}",
    # ),
    (
        r"Mg_2600images__",
        None,
        f"{DATASET_NAME}__Mg_2600_images",
        f"Structures of Mg from the 2600images split of {DATASET_NAME}",
    ),
    (
        r"Ni__",
        None,
        f"{DATASET_NAME}__Ni",
        f"Structures of Ni from {DATASET_NAME}",
    ),
    (
        r"Si_4600images__",
        None,
        f"{DATASET_NAME}__Si_4600_images",
        f"Structures of Si from the 4600images split of {DATASET_NAME}",
    ),
]


class Image(object):
    def __init__(
        self,
        atom_type=None,
        atom_num=None,
        iteration=None,
        Etot=None,
        Ep=None,
        Ek=None,
        scf=None,
    ) -> None:
        self.atom_num = atom_num
        self.iteration = iteration
        self.atom_type = []
        self.Etot = Etot
        self.Ep = Ep
        self.Ek = Ek
        self.scf = scf
        self.lattice = []
        self.stress = []
        self.position = []
        self.force = []
        self.atomic_energy = []
        self.content = []

    def set_md_info(
        self,
        method=None,
        time=None,
        temp=None,
        desired_temp=None,
        avg_temp=None,
        time_interval=None,
        tot_temp=None,
    ):
        self.method = method
        self.time = time
        self.temp = temp
        self.desired_temp = desired_temp
        self.avg_temp = avg_temp
        self.time_interval = time_interval
        self.tot_temp = tot_temp

    def set_energy_info(self, energy_content):
        numbers = re.findall(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", energy_content)
        self.atom_num = int(numbers[0])
        self.iteration = format(float(numbers[1]), ".2f")
        self.Etot, self.Ep, self.Ek = (
            float(numbers[2]),
            float(numbers[3]),
            float(numbers[4]),
        )
        if len(numbers) >= 5:
            self.scf = int(numbers[5])
        # self.content.append(energy_content)

    def set_lattice_stress(self, lattice_content):
        lattic1 = [
            float(_)
            for _ in re.findall(
                r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", lattice_content[0]
            )
        ]
        lattic2 = [
            float(_)
            for _ in re.findall(
                r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", lattice_content[1]
            )
        ]
        lattic3 = [
            float(_)
            for _ in re.findall(
                r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", lattice_content[2]
            )
        ]
        if "stress" in lattice_content[0]:
            self.stress = [lattic1[3:], lattic2[3:], lattic3[3:]]
        self.lattice = [lattic1[:3], lattic2[:3], lattic3[:3]]
        # self.content.append(lattice_content)

    def set_position(self, position_content):
        atom_type = []
        for i in range(0, len(position_content)):
            numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", position_content[i])
            atom_type.append(int(numbers[0]))
            self.position.append([float(_) for _ in numbers[1:4]])
        counter = Counter(atom_type)
        self.atom_type = atom_type
        self.atom_type_num = list(counter.values())
        assert self.atom_num == sum(self.atom_type_num)
        # self.content.append(position_content)

    def set_force(self, force_content):
        for i in range(0, len(force_content)):
            numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", force_content[i])
            self.force.append([float(_) for _ in numbers[1:4]])
        assert self.atom_num == len(self.force)

    def set_input(self, input_content):
        self.input = input_content

    def set_pressure(self, pressure_content):
        pressure1 = [
            float(_)
            for _ in re.findall(
                r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", pressure_content[0]
            )
        ]
        pressure2 = [
            float(_)
            for _ in re.findall(
                r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", pressure_content[1]
            )
        ]
        pressure3 = [
            float(_)
            for _ in re.findall(
                r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", pressure_content[2]
            )
        ]
        self.pressure = [pressure1, pressure2, pressure3]

    def set_atomic_energy(self, atomic_energy):
        for i in range(0, len(atomic_energy)):
            numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", atomic_energy[i])
            self.atomic_energy.append(float(numbers[1]))
        assert self.atom_num == len(self.atomic_energy)

    def set_content(self, content):
        self.content = content


class MOVEMENT(object):
    def __init__(self, movement_file: Path) -> None:
        self.movement_file = movement_file

    """Adapted from https://github.com/LonxunQuantum/PWMLFF_library/tree/main"""

    def load_movement_file(self):
        with open(self.movement_file, "r") as rf:
            mvm_contents = rf.readlines()
        ix = 0
        i = 0
        while i < len(mvm_contents):
            if "Iteration" in mvm_contents[i]:
                image = Image()
                image.set_energy_info(mvm_contents[i])
                i += 1
            elif "Lattice" in mvm_contents[i]:
                # three line for lattic info
                image.set_lattice_stress(mvm_contents[i + 1 : i + 4])  # noqa E203
                i += 4
            elif "MD_INFO" in mvm_contents[i]:
                md_keys = mvm_contents[i].strip().split()[1:]
                md_values = mvm_contents[i + 1].strip().split()
                input = {}
                for k, v in zip(md_keys, md_values):
                    try:
                        if "." in v:
                            input[k] = float(v)
                        else:
                            input[k] = int(v)
                    except Exception:
                        input[k] = v
                image.set_input(input)
                i += 2
            elif " Position" in mvm_contents[i]:
                image.set_position(mvm_contents[i + 1 : i + image.atom_num + 1])
                i = i + 1 + image.atom_num
            elif "Force" in mvm_contents[i]:
                image.set_force(mvm_contents[i + 1 : i + image.atom_num + 1])
                i = i + 1 + image.atom_num
            # elif "Atomic-Energy" in mvm_contents[i]:
            #     image.set_atomic_energy(mvm_contents[i + 1 : i + image.atom_num + 1])
            #     i = i + 1 + image.atom_num
            elif "Pressure Internal" in mvm_contents[i]:
                # in hartree/bohr^3
                image.set_pressure(mvm_contents[i + 1 : i + 4])
                i = i + 4
            else:
                i = i + 1  # to next line
            if "-------------" in mvm_contents[i]:
                labels = []
                if (
                    self.movement_file.parts[-2].endswith("k")
                    and self.movement_file.parts[-2][:-1].isdigit()
                ):
                    labels.append(f"temperature:{self.movement_file.parts[-2][:-1]}")
                labels.append(f"frame:{ix}")
                info = {
                    "forces": image.force,
                    "stress": image.pressure,
                    "energy": image.Etot,
                    "_labels": labels,
                    "_name": "__".join(
                        self.movement_file.parts[
                            self.movement_file.parts.index("available_data") + 1 :
                        ]
                    )
                    + f"__{ix}",
                    "input": image.input,
                }
                config = AtomicConfiguration(
                    positions=image.position,
                    cell=image.lattice,
                    symbols=image.atom_type,
                    info=info,
                )
                yield config
                ix += 1
                i = i + 1


def read_dir(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    data_paths = sorted(list(dir_path.rglob(GLOB_STR)))
    print(data_paths)
    for data_path in data_paths:
        print(f"Reading {data_path}")
        m = MOVEMENT(data_path)
        data_reader = m.load_movement_file()
        for config in data_reader:
            yield config


config_generator = read_dir(DATASET_FP)

dm = DataManager(
    nprocs=1,
    configs=config_generator,
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
    prop_map=PROPERTY_MAP,
    dataset_id=ds_id,
    read_write_batch_size=10000,
    standardize_energy=True,
)

print("Loading configurations")
dm.load_co_po_to_vastdb(loader)

print("Creating configuration sets")
config_set_rows = dm.create_configuration_sets(
    loader,
    CONFIGURATION_SETS,
)
print(config_set_rows)
print("Creating dataset")
dm.create_dataset(
    loader,
    name=DATASET_NAME,
    authors=AUTHORS,
    publication_link=PUBLICATION_LINK,
    data_link=DATA_LINK,
    description=DS_DESCRIPTION,
    labels=DS_LABELS,
    data_license=LICENSE,
    other_links=OTHER_LINKS,
)
# If running as a script, include below to stop the spark instance
#     loader.stop_spark()


# if __name__ == "__main__":
#     main()

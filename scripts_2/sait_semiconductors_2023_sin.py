"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
Lattice
Properties=species:S:1:pos:R:3:
forces:R:3
energy
stress
free_energy
pbc

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/sait_semiconductors")
DATASET_NAME = "SAIT_semiconductors_ACS_2023_SiN"

LICENSE = "https://creativecommons.org/licenses/by/4.0/"
SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://openreview.net/forum?id=hr9Bd1A9Un"
DATA_LINK = "https://github.com/SAITPublic/MLFF-Framework"
LINKS = [
    "https://openreview.net/forum?id=hr9Bd1A9Un",
    "https://github.com/SAITPublic/MLFF-Framework",
]
AUTHORS = [
    "Geonu Kim",
    "Byunggook Na",
    "Gunhee Kim",
    "Hyuntae Cho",
    "Seung-Jin Kang",
    "Hee Sun Lee",
    "Saerom Choi",
    "Heejae Kim",
    "Seungwon Lee",
    "Yongdeok Kim",
]
DATASET_DESC = (
    "This dataset contains SiN, Si and N configurations from the SAIT semiconductors "
    "datasets. SAIT semiconductors datasets comprise two rich datasets for "
    "the important semiconductor thin film materials silicon "
    "nitride (SiN) and hafnium oxide (HfO), gathered for the development "
    "of MLFFs. DFT simulations were conducted under various "
    "conditions that include differing initial structures, stoichiometry, "
    "temperature, strain, and defects."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "input": {
        "value": {
            "ediff": {"value": 2e-3, "units": "eV"},
            "ediffg": {"value": 5e-2, "units": "eV/angstrom"},
        }
    }
    # "basis-set": {"field": "basis_set"}
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/angstrom^3"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CSS = [
    [
        f"{DATASET_NAME}_N",
        {"names": {"$regex": "SiN_raw__N_Only"}},
        "Configurations of N (without Si) from the raw data for the "
        f"{DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_Si",
        {"names": {"$regex": "SiN_raw__Si_Only"}},
        "Configurations of Si (without N) from the raw data for the "
        f"{DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_SiN",
        {"names": {"$regex": "SiN_raw__SiN_compound"}},
        f"Configurations of SiN from the raw data for the {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_raw_OOD_melt",
        {"names": {"$regex": "SiN_raw__OOD__Melt"}},
        "Configurations of SiN from the melt portion of the out-of-domain structures "
        f"simulation from the raw data for the {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_raw_OOD_quench",
        {"names": {"$regex": "SiN_raw__OOD__Quench"}},
        "Configurations of SiN from the quench portion of the out-of-domain structures "
        f"simulation from the raw data for the {DATASET_NAME} dataset.",
    ],
    [
        f"{DATASET_NAME}_raw_OOD_relax",
        {"names": {"$regex": "SiN_raw__OOD__Relax"}},
        "Configurations of SiN from the relax portion of the out-of-domain structures "
        f"simulation from the raw data for the {DATASET_NAME} dataset.",
    ],
]

DSS = (
    (
        f"{DATASET_NAME}_train",
        DATASET_FP / "SiN",
        # {"name": {"$regex": "_Trainset"}},
        "Trainset.xyz",
        f"Training configurations from the {DATASET_NAME} dataset. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_out-of-domain",
        DATASET_FP / "SiN",
        # {"name": {"$regex": "_Trainset"}},
        "OOD.xyz",
        f"Out-of-domain configurations from the {DATASET_NAME} "
        "dataset. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_test",
        DATASET_FP / "SiN",
        # {"name": {"$regex": "_Testset"}},
        "Testset.xyz",
        f"Test configurations from the {DATASET_NAME} dataset. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_validation",
        DATASET_FP / "SiN",
        # {"name": {"$regex": "_Validset"}},
        "Validset.xyz",
        f"Validation configurations from the {DATASET_NAME} dataset. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_raw",
        DATASET_FP / "SiN_raw",
        # {"name": {"$regex": "_raw"}},
        "*.xyz",
        f"Structures from the {DATASET_NAME} dataset, separated into N-only, "
        "Si-only, SiN, and out-of-domain melt, quench and relax configuration "
        "sets. " + DATASET_DESC,
    ),
)


def reader(fp: Path):
    configs = read(fp, index=":")
    name = (
        "__".join(fp.parts[-4:])
        .replace("data__", "")
        .replace(".xyz", "")
        .replace("_MeltQuenchRelax", "")
    )
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
    return configs


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(free_energy_pd)
    for ds_name, ds_fp, ds_glob, ds_desc in DSS:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=ds_fp,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=ds_glob,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                # co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))
        # Create config sets for the one dataset
        if ds_name == f"{DATASET_NAME}_raw":
            cs_ids = []
            for i, (name, query, desc) in enumerate(CSS):
                cs_id = client.query_and_insert_configuration_set(
                    co_hashes=all_co_ids,
                    ds_id=ds_id,
                    name=name,
                    description=desc,
                    query=query,
                )

                cs_ids.append(cs_id)
            client.insert_dataset(
                do_hashes=all_do_ids,
                ds_id=ds_id,
                name=ds_name,
                authors=AUTHORS,
                links=[PUBLICATION, DATA_LINK],
                description=ds_desc,
                verbose=True,
                cs_ids=cs_ids,
                data_license=LICENSE,
            )

        # Other datasets have no CS divisions
        else:
            client.insert_dataset(
                do_hashes=all_do_ids,
                ds_id=ds_id,
                name=ds_name,
                authors=AUTHORS,
                links=[PUBLICATION, DATA_LINK],
                description=ds_desc,
                verbose=True,
                data_license=LICENSE,
            )


if __name__ == "__main__":
    main(sys.argv[1:])

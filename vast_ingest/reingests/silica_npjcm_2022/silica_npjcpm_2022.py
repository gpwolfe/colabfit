"""
authors: Gregory Wolfe, Alexander Tao

File notes
----------
configuration types (sets):{'bulk_amo', 'bulk_cryst', 'quench', 'cluster', 'liquid'}

virials and cauchy stress are present, but 210 of the config stress values are 6-size
tensors instead of 9. For this reason, stress is being included in the CO metadata and
virial is being included as the property instances

info keys

'config_type',
 'energy',
 'free_energy',
 'simulation_state',
 'stress',
 'sub_simulation_type',
 'virials'

arrays keys

forces

Notes on reingest:

data located in data_in_colabfit_data_snapshot_scripts1_2023_01_19.txt
"""

import os
from pathlib import Path
from time import time

from ase.io import iread
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    energy_pd,
    cauchy_stress_pd,
)

# Set up data loader environment
load_dotenv()
loader = SparkDataLoader(
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

# Define which tables will be used
loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage2"
loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids"

# loader.config_table = "ndb.colabfit.dev.co_silica_npjcm_2"
# loader.config_set_table = "ndb.colabfit.dev.cs_silica_npjcm_2"
# loader.dataset_table = "ndb.colabfit.dev.ds_silica_npjcm_2"
# loader.prop_object_table = "ndb.colabfit.dev.po_silica_npjcm_2"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)
ds_id = "DS_14m394gnh3ae_0"
DOI = "10.60732/c2bee5fa"

# DS_PATH = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/silica")
DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/data/silica_npjcm_2022/database/dataset.scan.2.xyz"
)
DATASET_NAME = "Silica_NPJCM_2022"
AUTHORS = ["Linus C. Erhard", "Jochen Rohrer", "Karsten Albe", "Volker L. Deringer"]

PUBLICATION = "https://doi.org/10.1038/s41524-022-00768-w"
DATA_LINK = "https://doi.org/10.5281/zenodo.6353683"
LINKS = [
    "https://doi.org/10.1038/s41524-022-00768-w",
    "https://doi.org/10.5281/zenodo.6353683",
]
DESCRIPTION = (
    "This dataset was created for the purpose of training an MLIP for silica (SiO2). "
    "For initial DFT computations, GPAW (in combination with ASE) was used with LDA, "
    "PBE and PBEsol functionals; and VASP with the SCAN functional. All calculations "
    "used the projector augmented-wave method. After comparison, it was found that "
    "SCAN performed best, and all values were recalculated using SCAN. An energy "
    "cut-off of 900 eV and a k-spacing of 0.23 Å-1 were used."
)

LICENSE = "CC-BY-4.0"
PUBLICATION_YEAR = "2022"

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "SCAN"},
    "input": {
        "value": {
            "encut": {"value": 900, "unit": "eV"},
            "kspacing": {"value": 0.23, "unit": "Ang^-1"},
        }
    },
}

PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        }
    ],
    cauchy_stress_pd["property-name"]: [
        {
            "stress": {"field": "stress", "units": "kB"},
            "volume-normalized": {"value": False, "units": None},
        },
    ],
    "_metadata": PI_MD,
}
# CO_METADATA = {
#     key: {"field": key}
#     for key in [
#         "config_type",
#         "simulation_state",
#         "sub_simulation_type",
#     ]
# }


def reader(fp):
    for i, config in enumerate(iread(fp, index=":")):
        if config.info.get("virials") is not None:
            if len(config.info["virials"]) == 9:
                config.info["virials"] = config.info["virials"].reshape((3, 3))
            else:
                virials = config.info["virials"]
                config.info["virials"] = [
                    [virials[0], virials[5], virials[4]],
                    [virials[5], virials[1], virials[3]],
                    [virials[4], virials[3], virials[2]],
                ]
        if config.arrays.get("stress") is not None:
            config.info["stress"] = config.arrays.pop("stress")
        if config.info.get("stress") is not None:
            if config.info["stress"].shape != (3, 3):
                stress = config.info["stress"]
                config.info["stress"] = [
                    [stress[0], stress[5], stress[4]],
                    [stress[5], stress[1], stress[3]],
                    [stress[4], stress[3], stress[2]],
                ]
        name = ""
        if config.info.get("config_type") is not None:
            name += f"{config.info['config_type']}__"
        if config.info.get("simulation_state") is not None:
            name += f"{config.info['simulation_state']}__"
        if config.info.get("sub_simulation_type") is not None:
            name += f"{config.info['sub_simulation_type']}__"
        name = name + str(i)
        config.info["_name"] = name
        yield AtomicConfiguration.from_ase(config)


"""
        Args for name_label_match in order:
        1. Regex pattern for matching CONFIGURATION NAMES
        2. Regex pattern for matching CONFIGURATION LABELS
        3. Name for configuration set
        4. Description for configuration set
        """
css = {
    (
        "bulk_amo",
        None,
        f"{DATASET_NAME}_SiO2_bulk_amorphous",
        "SiO2 snapshot taken after equilibration of the amorphous phase.",
    ),
    (
        "bulk_cryst",
        None,
        f"{DATASET_NAME}_SiO2_bulk_crystal",
        "SiO2 structures in crystalline phase",
    ),
    (
        "quench",
        None,
        f"{DATASET_NAME}_SiO2_quench_phase",
        "SiO2 snapshots taken during the quenching process",
    ),
    (
        "cluster",
        None,
        f"{DATASET_NAME}_SiO2_small_cluster",
        "SiO2 dimers and configurations with added oxygen clusters",
    ),
    (
        "liquid",
        None,
        f"{DATASET_NAME}_SiO2_liquid_phase",
        "SiO2 snapshots taken after equilibrating the liquid phase",
    ),
}


def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    if not DATASET_FP.exists():
        print(f"Path {DATASET_FP} does not exist")
        return
    config_generator = reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=ds_id,
        standardize_energy=True,
        read_write_batch_size=100000,
    )
    print(f"Time to prep: {time() - beg}")
    # t = time()
    # dm.load_co_po_to_vastdb(loader, batching_ingest=False)
    # print(f"Time to load: {time() - t}")
    # print("Creating dataset")
    t = time()
    dm.create_configuration_sets(loader, css)
    print(f"Time to create configuration sets: {time() - t}")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
        doi=DOI,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    main()

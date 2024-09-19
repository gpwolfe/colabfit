"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
func = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

QE-TB property keys

['atoms',
 'crystal_system',
 'energy_per_atom', <- potential energy, per-atom=True eV
 'f_enp',           <- formation energy eV
 'final_energy',    <- potential energy, per-atom=False eV
 'forces',          <- atomic forces eV/A
 'formula',
 'indir_gap',       <- band gap eV
 'jid',
 'natoms',
 'source_folder',
 'spacegroup_number',
 'spacegroup_symbol',
 'stress']          <- cauchy stress eV/A
"""

import json
import os
from pathlib import Path
from time import time
from ase.atoms import Atoms
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    SparkDataLoader,
)
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
    band_gap_pd,
    formation_energy_pd,
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


# loader.config_table = "ndb.colabfit.dev.co_jq"
# loader.config_set_table = "ndb.colabfit.dev.cs_jq"
# loader.dataset_table = "ndb.colabfit.dev.ds_jq"
# loader.prop_object_table = "ndb.colabfit.dev.po_jq"

loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage5"
loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids_stage4"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)
DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/jarvis_json/jqe_tb_folder.json"  # noqa
)
DATASET_NAME = "JARVIS_QE_TB"
DESCRIPTION = (
    "The QE-TB dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) DFT database. This subset contains "
    "configurations generated in Quantum ESPRESSO. "
    "JARVIS is a set of tools and datasets built to meet current materials design "
    "challenges."
)
DOI = "10.60732/9e9e5b29"
PUBLICATION_YEAR = "2023"
DATASET_ID = "DS_e471qdt7c6db_0"
LICENSE = "CC-BY-4.0"

PUBLICATION = "https://doi.org/10.1103/PhysRevMaterials.7.044603"
DATA_LINK = "https://ndownloader.figshare.com/files/29070555"
OTHER_LINKS = ["https://jarvis.nist.gov/", "https://arxiv.org/abs/2112.11585"]
LINKS = [
    "https://arxiv.org/abs/2112.11585",
    "https://doi.org/10.1103/PhysRevMaterials.7.044603",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/29070555",
]
AUTHORS = ["Kevin F. Garrity", "Kamal Choudhary"]
PI_MD = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBEsol"},
    "keys": {
        "value": {
            formation_energy_pd["property-name"]: "f_enp",
            band_gap_pd["property-name"]: "indir_gap",
            energy_pd["property-name"]: "final_energy",
            cauchy_stress_pd["property-name"]: "stress",
            atomic_forces_pd["property-name"]: "forces",
        },
    },
    "input": {
        "value": {
            "encut": {"value": "45", "units": "rydberg"},
            "pseudopotentials": "GBRV",
            "k-point-grid-density": {"value": 29, "units": "Ang^-1"},
            "smearing": "Gaussian",
            "smearing-energy": {"value": 0.01, "units": "rydberg"},
        }
    },
}

PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "final_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        },
    ],
    formation_energy_pd["property-name"]: [
        {
            "energy": {"field": "f_enp", "units": "eV"},
            "per-atom": {"value": True, "units": None},
        }
    ],
    cauchy_stress_pd["property-name"]: [
        {
            "stress": {"field": "stress", "units": "eV/angstrom^3"},
            "volume-normalized": {"value": False, "units": None},
        }
    ],
    band_gap_pd["property-name"]: [
        {
            "energy": {"field": "indir_gap", "units": "eV"},
            "type": {"value": "indirect", "units": None},
        },
    ],
    atomic_forces_pd["property-name"]: [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
        }
    ],
    "_metadata": PI_MD,
}


CO_KEYS = [
    # "atoms",            <- atom structures
    "crystal_system",
    #  'energy_per_atom', <- potential energy, per-atom=True eV
    #  'f_enp',           <- formation energy eV
    #  'final_energy',    <- potential energy, per-atom=False eV
    #  'forces',          <- atomic forces eV/A
    # "formula",
    #  'indir_gap',       <- band gap eV
    "jid",
    # "natoms",
    "source_folder",
    "spacegroup_number",
    "spacegroup_symbol",
    #  'stress'           <- cauchy stress eV/A
]

CO_METADATA = {key: {"field": key} for key in CO_KEYS}


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
    for i, row in enumerate(data):
        atoms = row.pop("atoms")
        if atoms["cartesian"] is True:
            config = Atoms(
                positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        else:
            config = Atoms(
                scaled_positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        name_parts = []
        for key in [
            "crystal_system",
            "formula",
            "spacegroup_number",
            "spacegroup_symbol" "jid",
        ]:
            if row.get(key):
                name_parts.append(str(row[key]))
        if len(name_parts) > 0:
            name = "__".join(name_parts)
        config.info["_name"] = f"{fp.stem}__{name}__{i}"
        for key, val in row.items():
            if key in ["indir_gap", "f_enp", "final_energy"]:
                config.info[key] = float(val)
            elif isinstance(val, str) and val != "na" and len(val) > 0:
                config.info[key] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key] = val
            elif isinstance(val, dict) and all([v != "na" for v in val.values()]):
                config.info[key] = val
            elif isinstance(val, float) or isinstance(val, int):
                config.info[key] = val
            else:
                pass
        yield AtomicConfiguration.from_ase(config, co_md_map=CO_METADATA)


# def main(range: tuple):
def main():
    beg = time()
    config_generator = reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[
            energy_pd,
            atomic_forces_pd,
            band_gap_pd,
            formation_energy_pd,
            cauchy_stress_pd,
        ],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
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

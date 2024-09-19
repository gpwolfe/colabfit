"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
fund = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
optb88vdw_total_energy (dft_3d)
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

keys:

['atoms',
 'band_gap',
 'density',
 'desc',
 'diel',
 'e_above_hull',
 'elasticity',
 'elements',
 'energy',
 'energy_per_atom',
 'formation_energy_per_atom',
 'full_formula',
 'hubbards',
 'icsd_id',
 'icsd_ids',
 'id',
 'is_compatible',
 'is_hubbard',
 'material_id',
 'nelements',
 'nsites',
 'oxide_type',
 'piezo',
 'pretty_formula',
 'spacegroup',
 'tags',
 'task_ids',
 'total_magnetization',
 'unit_cell_formula',
 'volume']
"""

import json
import os
from pathlib import Path
from time import time

from ase import Atoms
from dotenv import load_dotenv

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    SparkDataLoader,
)
from colabfit.tools.property_definitions import (
    band_gap_pd,
    energy_pd,
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


# loader.config_table = "ndb.colabfit.dev.co_jmpall"
# loader.config_set_table = "ndb.colabfit.dev.cs_jmpall"
# loader.dataset_table = "ndb.colabfit.dev.ds_jmpall"
# loader.prop_object_table = "ndb.colabfit.dev.po_jmpall"

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
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/all_mp.json"
)
DATASET_NAME = "JARVIS_Materials_Project_2020"
DESCRIPTION = (
    "The JARVIS_Materials_Project_2020 dataset is part of the joint automated "
    "repository for various integrated simulations (JARVIS) DFT database. "
    "This subset contains 127,000 configurations of 3D materials from the Materials "
    "Project database. "
    "JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges."
)

DATASET_ID = "DS_gpsibs9f47k4_0"
DOI = "10.60732/8122ca50"
PUBLICATION_YEAR = 2023
LICENSE = "NIST-PD"

PUBLICATION = "https://doi.org/10.1063/1.4812323"
DATA_LINK = "https://ndownloader.figshare.com/files/26791259"
OTHER_LINKS = ["https://jarvis.nist.gov/"]
LINKS = [
    "https://doi.org/10.1063/1.4812323",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/26791259",
]
AUTHORS = [
    "Anubhav Jain",
    "Shyue Ping Ong",
    "Geoffroy Hautier",
    "Wei Chen",
    "William Davidson Richards",
    "Stephen Dacek",
    "Shreyas Cholia",
    "Dan Gunter",
    "David Skinner",
    "Gerbrand Ceder",
    "Kristin A. Persson",
]

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT"},
    "keys": {
        "value": {
            formation_energy_pd["property-name"]: "formation_energy_per_atom",
            band_gap_pd["property-name"]: "band_gap",
            energy_pd["property-name"]: "energy",
        },
    },
}

PROPERTY_MAP = {
    formation_energy_pd["property-name"]: [
        {
            "energy": {"field": "formation_energy_per_atom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
        }
    ],
    band_gap_pd["property-name"]: [
        {
            "energy": {"field": "band_gap", "units": "eV"},
            "type": {"value": "unknown", "units": None},
        },
    ],
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        },
    ],
    "_metadata": PI_MD,
}


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
        info = {}
        for key, val in row.items():
            if key in ["formation_energy_per_atom", "band_gap", "energy"]:
                info[key] = float(val)
            elif isinstance(val, str) and val != "na" and len(val) > 0:
                info[key] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                info[key] = val
            elif isinstance(val, dict) and all([v != "na" for v in val.values()]):
                info[key] = val
            elif isinstance(val, float) or isinstance(val, int):
                info[key] = val
            else:
                pass
        config.info["_name"] = f"{fp.stem}__{i}"
        config.info.update(info)
        yield AtomicConfiguration.from_ase(config, co_md_map=CO_METADATA)


CO_KEYS = [
    # "atoms",
    # "band_gap",
    "density",
    "desc",
    "diel",
    "e_above_hull",
    "elasticity",
    # "elements",
    # "energy",
    # "energy_per_atom",
    # "formation_energy_per_atom",
    "full_formula",
    "hubbards",
    "icsd_id",
    "icsd_ids",
    "id",
    "is_compatible",
    "is_hubbard",
    "material_id",
    # "nelements",
    # "nsites",
    "oxide_type",
    "piezo",
    "pretty_formula",
    "spacegroup",
    "tags",
    "task_ids",
    "total_magnetization",
    "unit_cell_formula",
    "volume",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[formation_energy_pd, band_gap_pd, energy_pd],
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
        other_links=OTHER_LINKS,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
        doi=DOI,
        # labels=LABELS,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

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

Polymer genome keys:
Original keys (from publication):

Source: VSharma_etal:NatCommun.5.4845(2014)
Class: organic_polymer_crystal
Label: Polyimide
Structure prediction method used: USPEX
Number of atoms: 32
Number of atom types: 4
Atom types: C H O N
Dielectric constant, electronic: 3.71475E+00
Dielectric constant, ionic: 1.54812E+00
Dielectric constant, total: 5.26287E+00
Band gap at the GGA level (eV): 2.05350E+00
Band gap at the HSE06 level (eV): 3.30140E+00
Atomization energy (eV/atom): -6.46371E+00
Volume of the unit cell (A^3): 2.79303E+02

Keys from JARVIS-polymer-genome
[
'atom_en',  <-- atomization energy/atom
 'atoms',
 'diel_elec',
 'diel_ion',
 'diel_tot',
 'gga_gap',  <-- gap calculated with rPW86 functional
 'hse_gap',  <-- gap calc'd with HSE06 functional
 'id',
 'label',
 'method',
 'src',
 'vol'
 ]

"""

import json
import os
import sys
from pathlib import Path
from time import time

from ase.io import iread
from ase.atoms import Atoms
from dotenv import load_dotenv
from numpy import isnan

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    VastDataLoader,
)
from colabfit.tools.property_definitions import atomization_energy_pd, band_gap_pd

# from colabfit.tools.property_definitions import potential_energy_pd
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

# Define which tables will be used


# loader.config_table = "ndb.colabfit.dev.co_jpg"
# loader.config_set_table = "ndb.colabfit.dev.cs_jpg"
# loader.dataset_table = "ndb.colabfit.dev.ds_jpg"
# loader.prop_object_table = "ndb.colabfit.dev.po_jpg"

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
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/jarvis_json/pgnome.json"
)

DATASET_NAME = "JARVIS-Polymer-Genome"
DESCRIPTION = (
    "The JARVIS-Polymer-Genome dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the Polymer Genome dataset, as created for the linked "
    "publication (Huan, T., Mannodi-Kanakkithodi, A., Kim, C. et al.). Structures "
    "were curated from existing sources and the original authors' works, removing "
    "redundant, identical structures before calculations, and removing redundant "
    "datapoints after calculations were performed. Band gap energies were calculated "
    "using two different DFT functionals: rPW86 and HSE06; atomization energy was "
    "calculated using rPW86. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

DOI = "10.60732/37f5fcea"
DATASET_ID = "DS_dbgckv1il6v7_0"
LICENSE = "CC-BY-4.0"
PUBLICATION_YEAR = "2023"
PUBLICATION = "https://doi.org/10.1038/sdata.2016.12"
DATA_LINK = "https://ndownloader.figshare.com/files/26809907"
OTHER_LINKS = ["https://jarvis.nist.gov/"]
LINKS = [
    "https://doi.org/10.1038/sdata.2016.12",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/26809907",
]

AUTHORS = [
    "Tran Doan Huan",
    "Arun Mannodi-Kanakkithodi",
    "Chiho Kim",
    "Vinit Sharma",
    "Ghanshyam Pilania",
    "Rampi Ramprasad",
]

ELEMENTS = None
PI_MD = {
    "software": {"value": "VASP"},
    "keys": {"field": "keys"},
    "method": {"field": "cf_method"},
    "input": {
        "value": {"encut": {"value": 400, "units": "eV"}},
        "prec": "Accurate",
        "kspacing-atomization-energy": {"value": 0.25, "units": "Ang^-1"},
        "kspacing-band-gap": {"value": 0.20, "units": "Ang^-1"},
        "kpoints-scheme": "Monkhorst-Pack",
        "ediffg": {"value": 0.01, "units": "eV/angstrom"},
    },
}

PROPERTY_MAP = {
    atomization_energy_pd["property-name"]: [
        {
            "energy": {"field": "atomization_energy", "units": "eV"},
            "per-atom": {"value": True, "units": None},
        },
    ],
    band_gap_pd["property-name"]: [
        {
            "energy": {"field": "band_gap", "units": "eV"},
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
        info = config.info.copy()
        for en_gap in ["gga_gap", "hse_gap"]:
            config.info = info.copy()
            for key, val in row.items():
                if isinstance(val, str) and val != "na" and len(val) > 0:
                    config.info[key.replace(" ", "-")] = val
                elif (
                    isinstance(val, list)
                    and len(val) > 0
                    and any([x != "" for x in val])
                ):
                    config.info[key.replace(" ", "-")] = val
                elif isinstance(val, dict) and not all(
                    [v != "na" for v in val.values()]
                ):
                    config.info[key.replace(" ", "-")] = val
                elif (isinstance(val, float) or isinstance(val, int)) and not isnan(
                    val
                ):
                    config.info[key.replace(" ", "-")] = val
                else:
                    pass
            config.info["jarvis_method"] = row["method"]
            config.info["jarvis_id"] = row["id"]
            config.info["_name"] = (
                f"{fp.stem}__{en_gap}__{row['id']}__{row['label']}__{i}"
            )
            print(config.info)
            if en_gap == "gga_gap":
                config.info["band_gap"] = row["gga_gap"]
                config.info["atomization_energy"] = row["atom_en"]
                config.info["cf_method"] = "DFT-rPW86"
                config.info["keys"] = {
                    band_gap_pd["property-name"]: "gga_gap",
                    atomization_energy_pd["property-name"]: "atom_en",
                }
            else:
                config.info["band_gap"] = row["hse_gap"]
                config.info["cf_method"] = "DFT-HSE06"
                config.info["keys"] = {band_gap_pd["property-name"]: "hse_gap"}
            yield AtomicConfiguration.from_ase(config, co_md_map=CO_METADATA)


CO_KEYS = [
    # "atom_en",
    # "atoms",
    "diel_elec",
    "diel_ion",
    "diel_tot",
    # "gga_gap",
    # "hse_gap",
    "jarvis_id",
    "label",
    "jarvis_method",
    "src",
    "vol",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = reader(DATASET_FP)
    # config_generator = wrapper(DATASET_FP, range)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[atomization_energy_pd, band_gap_pd],
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
    main()

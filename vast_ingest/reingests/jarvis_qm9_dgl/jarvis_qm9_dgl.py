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

Keys from JARVIS-qm9-dgl
['Cv',      <-- heat capacity
 'G',       <-- free energy at 298.15K
 'H',       <-- enthalpy
 'U',       <-- internal energy at 298.15K
 'U0',      <-- internal energy at 0K
 'alpha',   <-- isotropic polarizability
 'atoms',
 'gap',   <-- homo-lumo gap, presumably not band gap
 'homo',
 'id',
 'lumo',
 'mu',      <-- dipole moment
 'r2',      <-- elec. spatial extent
 'zpve']

"""

import json
import os
from pathlib import Path
from time import time

from ase.atoms import Atoms
from dotenv import load_dotenv
from numpy import isnan

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    VastDataLoader,
)
from colabfit.tools.property_definitions import (
    energy_pd,
)

# Set up data loader environment
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


# loader.config_table = "ndb.colabfit.dev.co_dgl"
# loader.config_set_table = "ndb.colabfit.dev.cs_dgl"
# loader.dataset_table = "ndb.colabfit.dev.ds_dgl"
# loader.prop_object_table = "ndb.colabfit.dev.po_dgl"

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
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/jarvis_json/qm9_dgl.json"  # noqa
)

DATASET_NAME = "JARVIS-QM9-DGL"
DESCRIPTION = (
    "The JARVIS-QM9-DGL dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the QM9 dataset, originally created as part of the datasets "
    "at quantum-machine.org, as implemented with the Deep Graph Library (DGL) Python "
    "package. Units for r2 (electronic spatial extent) are a0^2; for alpha (isotropic "
    "polarizability), a0^3; for mu (dipole moment), D; for Cv (heat capacity), "
    "cal/mol K. Units for all other properties are eV. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LICENSE = "CC-BY-4.0"
DATASET_ID = "DS_tat5i46x3hkr_0"
DOI = "10.60732/403cd4f2"
PUBLICATION_YEAR = "2023"

PUBLICATION = "https://doi.org/10.1038/sdata.2014.22"
DATA_LINK = "https://ndownloader.figshare.com/files/28541196"
OTHER_LINKS = [
    "https://jarvis.nist.gov/",
    "http://quantum-machine.org/datasets/",
    "https://docs.dgl.ai/en/0.8.x/generated/dgl.data.QM9Dataset.html",
]

# LINKS = [
#     "https://doi.org/10.1038/sdata.2014.22",
#     "https://jarvis.nist.gov/",
#     "http://quantum-machine.org/datasets/",
#     "https://docs.dgl.ai/en/0.8.x/generated/dgl.data.QM9Dataset.html",
#     "https://ndownloader.figshare.com/files/28541196",
# ]
AUTHORS = [
    "Raghunathan Ramakrishnan",
    "Pavlo O. Dral",
    "Matthias Rupp",
    "O. Anatole von Lilienfeld",
]

PI_MD = {
    "software": {"value": "Gaussian 09"},
    "method": {"value": "DFT-B3LYP"},
    "basis-set": {"value": "6-31G(2df,p)"},
    "energy-temperature": {"value": "0K"},
    "U0": {"field": "U0", "units": "eV"},
    "U": {"field": "U", "units": "eV"},
    "keys": {
        "value": {
            energy_pd["property-name"]: "G",
        }
    },
}
ADD_KEYS = [
    "Cv",  # <-- heat capacity
    # "G",  # <-- free energy at 298.15K
    "H",  # <-- enthalpy
    #  'U',       # <-- internal energy at 298.15K
    #  'U0',      # <-- internal energy at 0K
    "alpha",  # <-- isotropic polarizability
    # "atoms",
    "gap",  # <-- gap
    "homo",
    "id",
    "lumo",
    "mu",  # <-- dipole moment
    "r2",  # <-- elec. spatial extent
    "zpve",
]


PI_MD.update({key: {"field": key} for key in ADD_KEYS})

PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "G", "units": "eV"},
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
        config.info["_name"] = f"{fp.stem}__jarvisid_{row['id']}__{i}"
        for key, val in row.items():
            if isinstance(val, str) and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, dict) and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (isinstance(val, float) or isinstance(val, int)) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
            else:
                pass
        yield AtomicConfiguration.from_ase(config)


def main():
    beg = time()
    config_generator = reader(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd],
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

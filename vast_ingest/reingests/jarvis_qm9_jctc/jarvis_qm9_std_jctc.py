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

Keys
['Cv',      <-- heat capacity
 'G',       <-- Free energy at 298.15K
 'H',       <-- enthalpy
 'HOMO',
 'LUMO',
 'R2',      <--elec. spatial extent
 'SMILES',
 'SMILES_relaxed',
 'U',       <-- internal energy at 298.15K
 'U0',      <-- internal energy at 0K
 'ZPVE',
 'alpha',
 'atoms',
 'gap',     <-- gap
 'id',
 'mu',
 'omega1']
"""

import json
import os
from pathlib import Path
from time import time

from ase.atoms import Atoms
from dotenv import load_dotenv
from numpy import isnan

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import DataManager, SparkDataLoader
from colabfit.tools.property_definitions import energy_pd

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


# loader.config_table = "ndb.colabfit.dev.co_jctc"
# loader.config_set_table = "ndb.colabfit.dev.cs_jctc"
# loader.dataset_table = "ndb.colabfit.dev.ds_jctc"
# loader.prop_object_table = "ndb.colabfit.dev.po_jctc"

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
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/jarvis_json/qm9_std_jctc.json"  # noqa
)
DATASET_ID = "DS_jz1q9juw7ycj_0"
PUBLICATION_YEAR = "2023"
LICENSE = "CC-BY-4.0"
DOI = "10.60732/5935fa4d"
DATASET_NAME = "JARVIS_QM9_STD_JCTC"
DESCRIPTION = (
    "The JARVIS_QM9_STD_JCTC dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the QM9 dataset, originally created as part of the datasets "
    "at quantum-machine.org. Units for r2 (electronic spatial extent) are a\0^2; for "
    "alpha (isotropic polarizability), a\0^3; for mu (dipole moment), D; for Cv (heat "
    "capacity), cal/mol K. Units for all other properties are eV. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
    "For the first iteration of DFT calculations, Gaussian 09's default electronic "
    "and geometry thresholds have been used for all molecules. For those molecules "
    "which failed to reach SCF convergence ultrafine grids have been invoked within "
    "a second iteration for evaluating the XC energy contributions. Within a third "
    "iteration on the remaining unconverged molecules, we identified those which "
    "had relaxed to saddle points, and further tightened the SCF criteria using "
    "the keyword scf(maxcycle=200, verytight). All those molecules which still "
    "featured imaginary frequencies entered the fourth iteration using keywords, "
    "opt(calcfc, maxstep=5, maxcycles=1000). calcfc constructs a Hessian in the "
    "first step of the geometry relaxation for eigenvector following. Within the "
    "fifth and final iteration, all molecules which still failed to reach "
    "convergence, have subsequently been converged using opt(calcall, maxstep=1, "
    "maxcycles=1000)"
)

LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1038/sdata.2014.22"
DATA_LINK = "https://ndownloader.figshare.com/files/28715319"
OTHER_LINKS = ["https://jarvis.nist.gov/", "http://quantum-machine.org/datasets/"]
LINKS = [
    "https://doi.org/10.1038/sdata.2014.22",
    "https://jarvis.nist.gov/",
    "http://quantum-machine.org/datasets/",
    "https://ndownloader.figshare.com/files/28715319",
]
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
    "keys": {"value": {energy_pd["property-name"]: "G"}},
}


ADD_KEYS = [
    "Cv",  # <-- heat capacity
    # "G",  # <-- Free energy at 298.15K
    "H",  # <-- enthalpy
    "HOMO",
    "LUMO",
    "R2",  # <--elec. spatial extent
    "SMILES",
    "SMILES_relaxed",
    "U",  # <-- internal energy at 298.15K
    "U0",  # <-- internal energy at 0K
    "ZPVE",
    "alpha",
    # "atoms",
    "gap",  # <-- gap (homo-lumo (?))
    "id",
    "mu",
    "omega1",
]


ADD_MD = {key: {"field": key} for key in ADD_KEYS}

PI_MD.update(ADD_MD)


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "G", "units": "eV"},
            "per-atom": {"value": False, "units": None},
        },
    ],
    # "band-gap": [
    #     {
    #         "energy": {"field": "gap", "units": "eV"},
    #         "_metadata": {
    #             "software": {"value": "Gaussian 09"},
    #             "method": {"value": "DFT-B3LYP"},
    #             "basis-set": {"value": "6-31G(2df,p)"},
    #         },
    #     },
    # ],
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

    main()

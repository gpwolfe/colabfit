"""
authors: Gregory Wolfe, Alexander Tao

XYZ header
----------
Dimer charge, Dimer multiplicity, Momomer A charge, Monomer A multiplicity,
Momomer B charge, Monomer B multiplicity, # of atoms in Monomer A,
# of atoms in Monomer B, CCSD(T)/CBS,   CCSD(T)/haTZ,   MP2/haTZ,   MP2/CBS,
MP2/aTZ,   MP2/aQZ,   HF/haTZ,   HF/aTZ,   HF/aQZ,   SAPT2+/aDZ Tot,
SAPT2+/aDZ Elst,   SAPT2+/aDZ Exch,   SAPT2+/aDZ Ind,   SAPT2+/aDZ Disp

File notes
----------
All potential energy values for a variety of methods and basis sets are included in the
header of each XYZ file.

Other property values included in configuration metadata:
SAPT2+ elements (besides total value, which is included in potential energy values)
Monomer charges for monomer A and B seperately
Monomer multiplicity for monomer A and B seperately
Dimer charge
Dimer multiplicity
Number of atoms in each monomer
"""

import os
from pathlib import Path
from time import time

from ase.io.extxyz import read_xyz
from dotenv import load_dotenv

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

loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage2"
loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids"

# loader.config_table = "ndb.colabfit.dev.co_nenci"
# loader.config_set_table = "ndb.colabfit.dev.cs_nenci"
# loader.dataset_table = "ndb.colabfit.dev.ds_nenci"
# loader.prop_object_table = "ndb.colabfit.dev.po_nenci"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

# DATASET_FP = Path(
#     "/persistent/colabfit_raw_data/new_raw_datasets_2.0/nenci2021/"
#     "nenci2021/xyzfiles/"
# )
# DATASET_FP = Path().cwd().parent / ("data/nenci2021/xyzfiles")  # local
DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts"
    "/gw_scripts/data/data/nenci2021/xyzfiles"
)

ds_id = "DS_0j2smy6relq0_0"
DOI = "10.60732/5d2a1ceb"

DATASET_NAME = "NENCI-2021"
AUTHORS = [
    "Zachary M. Sparrow",
    "Brian G. Ernst",
    "Paul T. Joo",
    "Ka Un Lao",
    "Robert A. DiStasio, Jr",
]

PUBLICATION = "https://doi.org/10.1063/5.0068862"
DATA_LINK = (
    "https://pubs.aip.org/jcp/article-supplement/199609/zip/184303_1_supplements/"
)
LINKS = [
    "https://doi.org/10.1063/5.0068862",
]
LICENSE = "CC BY 4.0"
PUBLICATION_YEAR = 2023
DESCRIPTION = (
    "NENCI-2021 is a database of approximately 8000 benchmark Non-Equilibirum "
    "Non-Covalent Interaction (NENCI) energies performed on molecular dimers;"
    "intermolecular complexes of biological and chemical relevance with a "
    "particular emphasis on close intermolecular contacts. Based on dimers"
    "from the S101 database."
)
PROPS = [
    "dimer_charge",
    "dimer_multiplicity",
    "monomer_a_charge",
    "monomer_a_multiplicity",
    "monomer_b_charge",
    "monomer_b_multiplicity",
    "natoms_monomer_a",
    "natoms_monomer_b",
    "CCSD(T)/CBS",  # CBS
    "CCSD(T)/haTZ",  # heavy-aug-cc-pVTZ
    "MP2/haTZ",  # heavy-aug-cc-pVTZ
    "MP2/CBS",
    "MP2/aTZ",  # aug-cc-pVTZ
    "MP2/aQZ",  # aug-cc-pVQZ
    "HF/haTZ",  # heavy-aug-cc-pVTZ
    "HF/aTZ",  # aug-cc-pVTZ
    "HF/aQZ",  # aug-cc-pVQZ
    "SAPT2+",  # aug-cc-pVDZ
    "sapt_electrostatics",
    "sapt_exchange",
    "sapt_induction",
    "sapt_dispersion",
]
# field, method, basis set
field_meth_bas = [
    ("CCSD(T)/CBS", "CCSD(T)", "CBS"),
    ("CCSD(T)/haTZ", "CCSD(T)", "heavy-aug-cc-pVTZ"),
    ("MP2/haTZ", "MP2", "heavy-aug-cc-pVTZ"),
    ("MP2/CBS", "MP2", "CBS"),
    ("MP2/aTZ", "MP2", "aug-cc-pVTZ"),
    ("MP2/aQZ", "MP2", "aug-cc-pVQZ"),
    ("HF/haTZ", "HF", "heavy-aug-cc-pVTZ"),
    ("HF/aTZ", "HF", "aug-cc-pVTZ"),
    ("HF/aQZ", "HF", "aug-cc-pVQZ"),
    ("SAPT2+", "SAPT2+", "aug-cc-pVDZ"),
]
co_md = {
    "dimer-charge": {"field": "dimer_charge"},
    "dimer-multiplicity": {"field": "dimer_multiplicity"},
    "monomer_a_charge": {"field": "momomer_a_charge"},
    "monomer_a_multiplicity": {"field": "monomer_a_multiplicity"},
    "monomer_b_charge": {"field": "momomer_b_charge"},
    "monomer_b_multiplicity": {"field": "monomer_b_multiplicity"},
    "num_atoms_monomer_a": {"field": "natoms_monomer_a"},
    "num_atoms_monomer_b": {"field": "natoms_monomer_b"},
    "SAPT2+/aDZ-electrostatics": {"field": "sapt_electrostatics"},
    "SAPT2+/aDZ-exchange": {"field": "sapt_exchange"},
    "SAPT2+/aDZ-induction": {"field": "sapt_induction"},
    "SAPT2+/aDZ-dispersion": {"field": "sapt_dispersion"},
}


def nenci_props_parser(string):
    s = [float(x) for x in string.split()]
    return dict(zip(PROPS, s))


PROPERTY_MAP = {
    energy_pd["property-name"]: [
        {
            "energy": {"field": "energy", "units": "kcal/mol"},
            "per-atom": {"value": False, "units": None},
        }
    ],
    "_metadata": {
        "software": {"value": "Psi4"},
        "method": {"field": "method"},
        "basis-set": {"field": "basis_set"},
    },
}


def reader(file_path):
    with open(file_path, "r") as f:
        atom = next(read_xyz(f, properties_parser=nenci_props_parser))
        info = atom.info.copy()
    for fmb in field_meth_bas:
        try:
            atom.info = info.copy()
            atom.info["energy"] = atom.info[fmb[0]]
            atom.info["method"] = fmb[1]
            atom.info["basis_set"] = fmb[2]
            atom.info["_name"] = f"{file_path.stem}__{fmb[0]}"
            # print(atom.info)
            yield AtomicConfiguration.from_ase(atom, co_md_map=co_md)
        except Exception as e:
            print(f"missing or empty keys for {fmb} in {file_path}")
            print(e)


def wrapper(dir_path: str):
    dir_path = Path(dir_path)
    if not dir_path.exists():
        print(f"Path {dir_path} does not exist")
        return
    xyz_paths = sorted(
        list(dir_path.rglob("*.xyz")),
        key=lambda x: f"{x.stem}",
    )
    for xyz_path in xyz_paths:
        print(f"Reading {xyz_path.name}")
        yield from reader(xyz_path)
        # for config in reader_func:
        #     yield config


def wrapper_wrapper(dir_path: str):
    yield from wrapper(dir_path)


CS_COMBOS = set()
for f in DATASET_FP.glob("*.xyz"):
    if not f.stem.startswith("."):
        CS_COMBOS.add(tuple(f.stem.split("_")[1].split("-")))

css = []
print(CS_COMBOS)
for i, (mon_a, mon_b) in enumerate(CS_COMBOS):
    # name match, label match, cs name, cs desciption
    css.append(
        (
            f"{mon_a}-{mon_b}",
            None,
            f"{DATASET_NAME}_{mon_a}-{mon_b}",
            f"From {DATASET_NAME}: Dimers containing {mon_a} as monomer A "
            f"and {mon_b} as monomer B",
        )
    )


def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = wrapper(DATASET_FP)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[energy_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=ds_id,
        standardize_energy=True,
        read_write_batch_size=100000,
    )
    print(f"Time to prep: {time() - beg}")
    t = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=False)
    print(f"Time to load: {time() - t}")
    # labels = LABELS
    t = time()
    dm.create_configuration_sets(loader, css)
    print(f"Time to create configuration sets: {time() - t}")
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
        # labels=labels,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    main()

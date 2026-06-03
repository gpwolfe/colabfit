"""
Halide_Double_Perovskite_Octahedral_Tilting ingest script for ColabFit VAST.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures)

File notes
----------
Source: vast_ingest/halide_double_perovskite_octahedral_tilting/
        halide_double_perovskite_octahedral_tilting/*.db
Three ASE databases (Cs2AgAlBr6.db, Cs2AgBiBr6.db, Cs2InBiBr6.db) contain the
DFT reference structures used to train neuroevolution potentials (NEP) for a
study of octahedral tilting in halide double perovskites. Each row carries
energy, forces, and stress in a SinglePointCalculator. Reference data were
generated with VASP using the SCAN+rVV10 meta-GGA functional
(METAGGA=SCAN, LUSE_VDW with BPARAM=15.7, CPARAM=0.0093), ENCUT=520 eV,
Gaussian smearing (ISMEAR=0, SIGMA=0.1), and Gamma-centered KSPACING=0.25
(the INCAR.NEP settings used to build the training set).
"""

import os
from pathlib import Path

import ase.db
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyInfo, PropertyMap
from dotenv import load_dotenv

load_dotenv()

access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")

loader = VastDataLoader(
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
loader.config_table = "ndb.colabfit.dev.co_hdp_tilt"
loader.config_set_table = "ndb.colabfit.dev.cs_hdp_tilt"
loader.dataset_table = "ndb.colabfit.dev.ds_hdp_tilt"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_hdp_tilt"

DATASET_ID = "DS_zqofv7mcsq1v_0"
DATASET_NAME = "Halide_Double_Perovskite_Octahedral_Tilting"
DESCRIPTION = (
    "DFT reference structures used to train neuroevolution potentials (NEP) "
    "for a study disentangling lone-pair chemistry and geometric effects in "
    "the octahedral tilting of halide double perovskites. The dataset contains "
    "the training configurations (with energies, forces, and stresses) for "
    "three representative compounds: Cs2AgAlBr6, Cs2AgBiBr6, and Cs2InBiBr6. "
    "Reference calculations used VASP with the SCAN+rVV10 meta-GGA functional "
    "(BPARAM=15.7, CPARAM=0.0093), a 520 eV plane-wave cutoff, Gaussian "
    "smearing (SIGMA=0.1 eV), and Gamma-centered Brillouin-zone sampling "
    "(KSPACING=0.25). Configuration sets group the structures by compound."
)

PUBLICATION = "https://doi.org/10.48550/arXiv.2605.19414"
DATA_LINK = "https://doi.org/10.5281/zenodo.18841767"
OTHER_LINKS = ["https://arxiv.org/abs/2605.19414"]
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Mehmet Baskurt",
    "Erik Fransson",
    "Madeleine Lindvik",
    "Paul Erhart",
    "Julia Wiktor",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "halide_double_perovskite_octahedral_tilting"


def reader(data_path: Path):
    for db_file in sorted(data_path.glob("*.db")):
        compound = db_file.stem
        db = ase.db.connect(db_file)
        for row in db.select():
            atoms = row.toatoms()
            # energy/forces/stress carried in atoms.calc.results from toatoms()
            atoms.info["_name"] = f"{DATASET_NAME}__{compound}__{row.id}"
            atoms.info["compound"] = compound
            yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-SCAN+rVV10")
property_map.set_metadata_field(
    "input",
    {
        "ENCUT": "520 eV",
        "EDIFF": "1e-5 eV",
        "ISMEAR": "0 (Gaussian)",
        "SIGMA": "0.1 eV",
        "kpoints": "Gamma-centered, KSPACING=0.25 Angstrom^-1",
        "van_der_waals": "rVV10 nonlocal correlation (BPARAM=15.7, CPARAM=0.0093)",
        "description": (
            "VASP SCAN+rVV10 meta-GGA used to generate the NEP training "
            "reference data (INCAR.NEP settings)."
        ),
    },
)
property_map.set_metadata_field("compound", "compound", dynamic=True)

energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)
force_info = PropertyInfo(
    property_name="atomic-forces",
    field="forces",
    units="eV/angstrom",
    original_file_key="forces",
)
stress_info = PropertyInfo(
    property_name="cauchy-stress",
    field="stress",
    units="eV/angstrom^3",
    original_file_key="stress",
    additional=[("volume-normalized", {"value": False, "units": None})],
)

property_map.set_properties([energy_info, force_info, stress_info])
PROPERTY_MAP = property_map.get_property_map()

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

dm.create_configuration_sets(
    loader=loader,
    name_label_match=[
        (
            f"{DATASET_NAME}__Cs2AgAlBr6__.*",
            None,
            f"{DATASET_NAME}_Cs2AgAlBr6",
            "NEP training reference structures for Cs2AgAlBr6.",
            False,
        ),
        (
            f"{DATASET_NAME}__Cs2AgBiBr6__.*",
            None,
            f"{DATASET_NAME}_Cs2AgBiBr6",
            "NEP training reference structures for Cs2AgBiBr6.",
            False,
        ),
        (
            f"{DATASET_NAME}__Cs2InBiBr6__.*",
            None,
            f"{DATASET_NAME}_Cs2InBiBr6",
            "NEP training reference structures for Cs2InBiBr6.",
            False,
        ),
    ],
)

dm.create_dataset(
    loader=loader,
    name=DATASET_NAME,
    authors=AUTHORS,
    publication_link=PUBLICATION,
    data_link=DATA_LINK,
    other_links=OTHER_LINKS,
    description=DESCRIPTION,
    data_license=LICENSE,
    publication_year=PUBLICATION_YEAR,
    equilibrium=False,
    date_requested="2026-06-02",
)

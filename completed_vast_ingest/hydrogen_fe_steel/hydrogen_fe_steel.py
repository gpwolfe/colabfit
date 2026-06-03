"""
Hydrogen_Fe_Steel ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures)

File notes
----------
Source: vast_ingest/hydrogen_fe_steel/hydrogen_fe_steel/<job>/Stage_1/OUTCAR.out
Spin-polarized VASP relaxations of a 55-atom bcc Fe-based hybrid-steel
supercell (VMoCrMnFe47NiAlSiC) with 0/1/2/5 interstitial hydrogen atoms,
probing hydrogen-induced lattice strain. All ionic steps of each
optimization are ingested (energy/forces/stress read from atoms.calc.results,
which from_ase preserves). VASP 6.4.2 (MedeA 3.8.1), GGA-PBE, ENCUT=400 eV,
EDIFF=5e-5 eV, ISPIN=2, Methfessel-Paxton smearing (SIGMA=0.2 eV), 2x2x2
Gamma-centered k-mesh, full cell+ion relaxation (IBRION=2, ISIF=3,
EDIFFG=-0.02 eV/Angstrom).

Job -> hydrogen content: 618=0H, 619=1H, 620=2H, 621=5H.

Supporting data for Aksoy et al., "Real-time mapping of hydrogen diffusion in
steel," Communications Materials (2026), doi:10.1038/s43246-026-01178-x.
"""

import os
from pathlib import Path

from ase.io import iread
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
loader.config_table = "ndb.colabfit.dev.co_h_fe_steel"
loader.config_set_table = "ndb.colabfit.dev.cs_h_fe_steel"
loader.dataset_table = "ndb.colabfit.dev.ds_h_fe_steel"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_h_fe_steel"

DATASET_ID = "DS_j30r8n9wcmko_0"
DATASET_NAME = "Hydrogen_Fe_Steel"
DESCRIPTION = (
    "Spin-polarized density functional theory structural relaxations probing "
    "hydrogen-induced lattice strain in a chemically complex Fe-based (bcc) "
    "hybrid steel. A 55-atom supercell of composition VMoCrMnFe47NiAlSiC is "
    "relaxed with 0, 1, 2, and 5 hydrogen atoms inserted into interstitial "
    "sites; all ionic steps of each cell-and-ion optimization are included, "
    "yielding configurations with total energies, atomic forces, and stresses. "
    "Calculations used VASP 6.4.2 (within the MedeA environment) with the "
    "GGA-PBE functional, a 400 eV plane-wave cutoff, spin polarization "
    "(ISPIN=2), Methfessel-Paxton smearing (SIGMA=0.2 eV), a 2x2x2 "
    "Gamma-centered k-point mesh, and full relaxation of lattice vectors and "
    "atomic positions (IBRION=2, ISIF=3, EDIFFG=-0.02 eV/Angstrom). "
    "Configuration sets group the relaxations by hydrogen content."
)

PUBLICATION = "https://doi.org/10.1038/s43246-026-01178-x"
DATA_LINK = "https://doi.org/10.5281/zenodo.19038479"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Ammar Aksoy",
    "Cem Örnek",
    "Beste Payam",
    "Bilgehan M. Şeşen",
    "Çağatay Yelkarası",
    "Steve Ooi",
]
# Data-repository (Zenodo 19038479) license; the journal article is CC BY-NC-ND-4.0.
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "hydrogen_fe_steel"

H_CONTENT = {
    "618": "0H",
    "619": "1H",
    "620": "2H",
    "621": "5H",
}


def reader(data_path: Path):
    for job, subset in sorted(H_CONTENT.items()):
        outcar = data_path / job / "Stage_1" / "OUTCAR.out"
        if not outcar.exists():
            print(f"Missing OUTCAR: {outcar}")
            continue
        for i, atoms in enumerate(iread(outcar, index=":", format="vasp-out")):
            # energy/forces/stress are read from atoms.calc.results,
            # which from_ase preserves; only _name must be set here.
            atoms.info["_name"] = f"{DATASET_NAME}__{subset}__step_{i}"
            atoms.info["h_content"] = subset
            yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP 6.4.2")
property_map.set_metadata_field("method", "DFT-PBE")
property_map.set_metadata_field(
    "input",
    {
        "functional": "GGA-PBE, spin-polarized (ISPIN=2)",
        "ENCUT": "400 eV",
        "EDIFF": "5e-5 eV",
        "ISMEAR": "1 (Methfessel-Paxton)",
        "SIGMA": "0.2 eV",
        "kpoints": "2x2x2 Gamma-centered Monkhorst-Pack",
        "relaxation": "IBRION=2, ISIF=3 (cell+ions), EDIFFG=-0.02 eV/Angstrom",
        "description": (
            "VASP 6.4.2 within MedeA 3.8.1; full relaxation of a 55-atom bcc "
            "Fe-based hybrid-steel supercell with interstitial hydrogen."
        ),
    },
)
property_map.set_metadata_field("h_content", "h_content", dynamic=True)

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

SUBSETS = {
    "0H": "Hydrogen-free reference hybrid-steel supercell (55 atoms).",
    "1H": "Hybrid-steel supercell with 1 interstitial hydrogen atom.",
    "2H": "Hybrid-steel supercell with 2 interstitial hydrogen atoms.",
    "5H": "Hybrid-steel supercell with 5 interstitial hydrogen atoms.",
}

CSS = [
    (
        f"{DATASET_NAME}__{subset}__.*",
        None,
        f"{DATASET_NAME}_{subset}",
        desc,
        False,
    )
    for subset, desc in SUBSETS.items()
]

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

dm.create_configuration_sets(loader=loader, name_label_match=CSS)

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

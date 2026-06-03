"""
Graphene_FET_Urea_Sensor ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV (first ionic-step E0 from OSZICAR)

File notes
----------
Source: vast_ingest/graphene_fet_urea_sensor/graphene_fet_urea_sensor/
        fig3/structures_and_energies/<structure>/{POSCAR, OSZICAR}
DFT structures and energies for graphene with urea and water molecules,
from a combined experimental/DFT study of a graphene field-effect-transistor
sensor for urea detection in water. Each subdirectory holds ONLY the input
geometry (POSCAR) and the relaxation log (OSZICAR) - no CONTCAR/XDATCAR/
vasprun.xml. OSZICAR carries a per-ionic-step E0 energy but no per-step
geometries, so the 110-step relaxation cannot be ingested as a trajectory and
the final (relaxed) energy has no matching archived geometry. The only
consistent (geometry, energy) pair is therefore the POSCAR (initial) geometry
with the FIRST ionic-step E0 from OSZICAR - the single-point energy of that
exact geometry. Calculations used VASP with the optB86b-vdW functional
(GGA=MK, LUSE_VDW=.TRUE., PARAM1=0.1234, PARAM2=1.0), ENCUT=900 eV, Gaussian
smearing (ISMEAR=0, SIGMA=0.01), and a 3x3x1 Monkhorst-Pack k-point mesh.
Forces and stresses are not provided.

Published in Applied Surface Science Advances 33 (2026) 100978. (Note: the
article text reports an 800 eV cutoff, but the archived INCAR files used to
generate this data specify ENCUT=900 eV; the data value is recorded here.)
"""

import os
from pathlib import Path

from ase.io import read
from colabfit.tools.property_definitions import energy_pd
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
loader.config_table = "ndb.colabfit.dev.co_graphene_urea"
loader.config_set_table = "ndb.colabfit.dev.cs_graphene_urea"
loader.dataset_table = "ndb.colabfit.dev.ds_graphene_urea"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_graphene_urea"

DATASET_ID = "DS_bgu331li4n73_0"
DATASET_NAME = "Graphene_Field_Effect_Transistor_Urea_Sensor"
DESCRIPTION = (
    "DFT-optimized structures and total energies of graphene interacting with "
    "urea and water molecules, supporting a combined experimental and "
    "first-principles study of a graphene field-effect-transistor (FET) sensor "
    "for the detection of urea in water. The configurations span graphene with "
    "one or more urea/water molecules in various adsorption geometries. "
    "Calculations used VASP with the optB86b-vdW exchange-correlation "
    "functional (GGA=MK, LUSE_VDW, PARAM1=0.1234, PARAM2=1.0), a 900 eV "
    "plane-wave cutoff, Gaussian smearing (SIGMA=0.01 eV), and a 3x3x1 "
    "Monkhorst-Pack k-point mesh. The relaxed geometries (CONTCAR) were not "
    "archived, so each input geometry (POSCAR) is paired with the energy of "
    "the first ionic step from OSZICAR - the single-point energy of that "
    "geometry."
)

PUBLICATION = "https://doi.org/10.1016/j.apsadv.2026.100978"
DATA_LINK = "https://doi.org/10.5281/zenodo.19236518"
OTHER_LINKS = None
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Ondřej Špaček",
    "Linda Supalová",
    "Jindřich Mach",
    "David Nezval",
    "Tomáš Šikola",
    "Miroslav Bartošík",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "graphene_fet_urea_sensor"


def _first_e0(oszicar_path: Path):
    """Return the first ionic-step E0 energy (eV) from a VASP OSZICAR file.

    The first ionic step is the SCF on the initial (POSCAR) geometry, so its
    E0 is the single-point energy of that exact geometry.
    """
    with open(oszicar_path) as f:
        for line in f:
            if "E0=" in line:
                tokens = line.split()
                return float(tokens[tokens.index("E0=") + 1])
    return None


def reader(data_path: Path):
    base = data_path / "fig3" / "structures_and_energies"
    for sub in sorted(p for p in base.iterdir() if p.is_dir()):
        poscar = sub / "POSCAR"
        oszicar = sub / "OSZICAR"
        if not (poscar.exists() and oszicar.exists()):
            continue
        energy = _first_e0(oszicar)
        if energy is None:
            print(f"No E0 in {oszicar}; skipping")
            continue
        atoms = read(poscar, format="vasp")
        atoms.info["_name"] = f"{DATASET_NAME}__{sub.name}"
        atoms.info["energy"] = energy
        yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd])

property_map.set_metadata_field("software", "VASP")
property_map.set_metadata_field("method", "DFT-optB86b-vdW")
property_map.set_metadata_field(
    "input",
    {
        "ENCUT": "900 eV",
        "EDIFF": "1e-5 eV",
        "EDIFFG": "-0.01 eV/Angstrom (force convergence)",
        "ISMEAR": "0 (Gaussian)",
        "SIGMA": "0.01 eV",
        "kpoints": "3x3x1 Monkhorst-Pack",
        "functional": "optB86b-vdW (GGA=MK, LUSE_VDW, PARAM1=0.1234, PARAM2=1.0)",
        "description": (
            "VASP optB86b-vdW van der Waals density functional; total energy is "
            "the first ionic-step E0 (energy without entropy) from OSZICAR, the "
            "single-point energy of the initial POSCAR geometry."
        ),
    },
)

energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units="eV",
    original_file_key="energy",
    additional=[("per-atom", {"value": False, "units": None})],
)

property_map.set_properties([energy_info])
PROPERTY_MAP = property_map.get_property_map()

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[energy_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

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

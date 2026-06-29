"""
curatedQM9 ingest script for ColabFit VAST database.

Properties
----------
energy: internal energy at 0 K (U0) in Hartree; auto-converted to eV on ingest.
    QM9 stores total internal energies (not atomization energies) in the xyz
    comment line; U0 is used here as the configuration energy.

File notes
----------
Source: 133660_curatedQM9_outof_133885/ (133,660 single-molecule .xyz files)
curatedQM9 is a cleaned release of the QM9 dataset. The original QM9 reports
B3LYP/6-31G(2df,p) equilibrium geometries and properties for 133,885 small
organic molecules (CHONF, up to nine heavy atoms). Of these, 3,054 molecules
had questionable geometric stability: their DFT-relaxed geometries no longer
matched the Lewis connectivity of their generating SMILES. Senthil, Chakraborty
and Ramakrishnan (Chem. Sci. 2021) used the ConnGO connectivity-preserving
geometry-optimization workflow to re-optimize these structures. This release
keeps the 130,830 unmodified original-QM9 geometries and adds 2,830 ConnGO
re-optimized geometries (filenames ending in "_new"), for 133,660 molecules
whose geometries are bijectively mapped to their intended Lewis formulae.

xyz format (QM9 Table 3, properties stored in column order)
-----------------------------------------------------------
Line 1: natoms
Line 2: tag/id then A B C mu alpha homo lumo gap r2 zpve U0 U H G Cv
Lines 3..n+2: element x y z [mulliken_charge]  (charge present in originals only)
Line n+3: harmonic vibrational frequencies (cm^-1)
Line n+4: SMILES (GDB-17 generated, B3LYP-relaxed)
Line n+5: InChI (GDB-17 generated, B3LYP-relaxed)
Per-atom: species, positions, mulliken_charges (originals only)
"""

import re
import os
from pathlib import Path

from ase import Atoms as AseAtoms
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
loader.config_table = "ndb.colabfit.dev.co_curatedqm9"
loader.config_set_table = "ndb.colabfit.dev.cs_curatedqm9"
loader.dataset_table = "ndb.colabfit.dev.ds_curatedqm9"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_curatedqm9"

DATASET_ID = "DS_9t02w6ml1m7y_0"
DATASET_NAME = "curatedQM9"

DESCRIPTION = (
    "curatedQM9 is a cleaned release of the QM9 dataset of small organic "
    "molecules. The original QM9 reports B3LYP/6-31G(2df,p) equilibrium "
    "geometries and a suite of quantum-chemical properties for 133,885 "
    "neutral, closed-shell molecules built from C, H, O, N, and F with up to "
    "nine heavy atoms (a subset of the GDB-17 chemical universe). Of those, "
    "3,054 molecules were flagged as having questionable geometric stability "
    "because their DFT-relaxed geometries no longer preserved the bonding "
    "connectivity (Lewis structure) of their generating SMILES. Senthil, "
    "Chakraborty, and Ramakrishnan (Chem. Sci., 2021) applied ConnGO, an "
    "iterative connectivity-preserving geometry-optimization workflow based on "
    "DFT and post-DFT methods, to repair these structures. curatedQM9 retains "
    "the 130,830 unmodified original-QM9 geometries and adds 2,830 molecules "
    "re-optimized with ConnGO at the same B3LYP/6-31G(2df,p) level of theory "
    "(file names ending in '_new'), yielding 133,660 molecules whose minimum-"
    "energy geometries are bijectively mapped to their intended Lewis "
    "formulae. Each record carries the QM9 scalar properties as metadata "
    "(rotational constants, dipole moment, isotropic polarizability, HOMO and "
    "LUMO energies, HOMO-LUMO gap, electronic spatial extent, zero-point "
    "vibrational energy, internal energies, enthalpy, free energy, heat "
    "capacity, harmonic frequencies, SMILES, and InChI); the configuration "
    "energy is the QM9 internal energy at 0 K (U0)."
)

PUBLICATION = "https://doi.org/10.1039/D0SC05591C"
DATA_LINK = "https://github.com/moldis-group/curatedQM9"
OTHER_LINKS = [
    "https://moldis-group.github.io/curatedQM9/",
    "https://doi.org/10.1038/sdata.2014.22",
]
PUBLICATION_YEAR = "2021"

AUTHORS = [
    "Salini Senthil",
    "Sabyasachi Chakraborty",
    "Raghunathan Ramakrishnan",
]
# Data are distributed under the MIT license of the moldis-group/curatedQM9
# GitHub repository; the Chemical Science article itself is CC BY-NC 3.0.
LICENSE = "MIT"
DOI = None

DATA_PATH = Path(__file__).parent / "133660_curatedQM9_outof_133885"

# Properties in the xyz comment line, in QM9 Table 3 column order (after the
# tag/index token). Units: A,B,C in GHz; mu in Debye; alpha in Bohr^3;
# homo,lumo,gap,zpve,U0,U,H,G in Hartree; r2 in Bohr^2; Cv in cal/(mol K).
PROP_KEYS = [
    "A", "B", "C", "mu", "alpha", "homo", "lumo", "gap",
    "r2", "zpve", "U0", "U", "H", "G", "Cv",
]

SUBSET_DESCRIPTIONS = {
    "original": (
        "Molecules retained unchanged from the original QM9 dataset; their "
        "B3LYP/6-31G(2df,p) equilibrium geometries already preserve the Lewis "
        "connectivity of their generating SMILES."
    ),
    "reoptimized": (
        "Molecules whose original QM9 geometries had questionable geometric "
        "stability and were re-optimized with the ConnGO connectivity-"
        "preserving workflow at the B3LYP/6-31G(2df,p) level (file names ending "
        "in '_new')."
    ),
}


# -----------------------------------------------------------------------------
# READER
# -----------------------------------------------------------------------------


def _to_float(token):
    """Parse a QM9 numeric token, handling Fortran-style exponents.

    QM9 files use '*^' (e.g. ``1.2*^-6``) and this release also uses 'E^'
    (e.g. ``2.1997E^-6``) to denote scientific notation.
    """
    return float(token.replace("*^", "e").replace("^", ""))


def reader(data_dir: Path):
    """Yield AtomicConfiguration objects from the curatedQM9 xyz files."""
    for fp in sorted(data_dir.glob("*.xyz")):
        stem = fp.stem
        reoptimized = stem.endswith("_new")
        subset = "reoptimized" if reoptimized else "original"
        match = re.search(r"(\d+)", stem)
        qm9_index = int(match.group(1)) if match else None

        lines = fp.read_text().splitlines()
        natoms = int(lines[0])
        prop_tokens = lines[1].split()[-15:]
        props = {k: _to_float(v) for k, v in zip(PROP_KEYS, prop_tokens)}

        symbols, positions, mulliken = [], [], []
        for line in lines[2:2 + natoms]:
            parts = line.split()
            symbols.append(parts[0])
            positions.append([_to_float(p) for p in parts[1:4]])
            if len(parts) >= 5:
                mulliken.append(_to_float(parts[4]))

        frequencies = [_to_float(x) for x in lines[2 + natoms].split()]
        smiles_tokens = lines[3 + natoms].split()
        inchi_tokens = lines[4 + natoms].split()

        atoms = AseAtoms(symbols=symbols, positions=positions, pbc=False)
        atoms.info = {
            "_name": f"{DATASET_NAME}__{subset}__{stem}",
            "energy": props["U0"],
            "subset": subset,
            "qm9_index": qm9_index,
            "A": props["A"],
            "B": props["B"],
            "C": props["C"],
            "mu": props["mu"],
            "alpha": props["alpha"],
            "homo": props["homo"],
            "lumo": props["lumo"],
            "gap": props["gap"],
            "r2": props["r2"],
            "zpve": props["zpve"],
            "U": props["U"],
            "H": props["H"],
            "G": props["G"],
            "Cv": props["Cv"],
            "frequencies": frequencies,
            "smiles": smiles_tokens[0],
            "smiles_relaxed": smiles_tokens[-1],
            "inchi": inchi_tokens[0],
            "inchi_relaxed": inchi_tokens[-1],
        }
        # Mulliken charges exist only in the original-QM9 files; store on
        # info (custom arrays are dropped by AtomicConfiguration.from_ase).
        if mulliken:
            atoms.info["mulliken_charges"] = mulliken
        yield AtomicConfiguration.from_ase(atoms)


# -----------------------------------------------------------------------------
# PROPERTY MAP
# -----------------------------------------------------------------------------

property_map = PropertyMap([energy_pd])

property_map.set_metadata_field("software", "Gaussian 09")
property_map.set_metadata_field("method", "DFT-B3LYP")
property_map.set_metadata_field(
    "input",
    {
        "functional": "B3LYP",
        "basis_set": "6-31G(2df,p)",
        "energy_type": "internal energy at 0 K (U0)",
        "description": (
            "QM9 equilibrium geometries and properties at the "
            "B3LYP/6-31G(2df,p) level. The 'reoptimized' subset was re-relaxed "
            "with the ConnGO connectivity-preserving workflow at the same level "
            "of theory. Metadata units: rotational constants A,B,C in GHz; "
            "dipole moment mu in Debye; isotropic polarizability alpha in "
            "Bohr^3; homo, lumo, gap, zpve, U, H, G in Hartree; electronic "
            "spatial extent r2 in Bohr^2; heat capacity Cv in cal/(mol K); "
            "harmonic frequencies in cm^-1."
        ),
    },
)
# Per-configuration metadata read from atoms.info / atoms.arrays at ingest.
# Fields absent for a given config (e.g. mulliken_charges in the reoptimized
# subset) are silently skipped.
for _field in [
    "subset", "qm9_index", "A", "B", "C", "mu", "alpha", "homo", "lumo",
    "gap", "r2", "zpve", "U", "H", "G", "Cv", "frequencies", "smiles",
    "smiles_relaxed", "inchi", "inchi_relaxed", "mulliken_charges",
]:
    property_map.set_metadata_field(_field, _field, dynamic=True)

energy_info = PropertyInfo(
    property_name="energy",
    field="energy",
    units="hartree",
    original_file_key="U0",
    additional=[("per-atom", {"value": False, "units": None})],
)
property_map.set_properties([energy_info])
PROPERTY_MAP = property_map.get_property_map()

# -----------------------------------------------------------------------------
# INGEST
# -----------------------------------------------------------------------------

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

dm.create_configuration_sets(
    loader=loader,
    name_label_match=[
        (
            f"{DATASET_NAME}__original__.*",
            None,
            f"{DATASET_NAME}_original",
            SUBSET_DESCRIPTIONS["original"],
            False,
        ),
        (
            f"{DATASET_NAME}__reoptimized__.*",
            None,
            f"{DATASET_NAME}_reoptimized",
            SUBSET_DESCRIPTIONS["reoptimized"],
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
    equilibrium=True,
    date_requested="2026-06-09",
)

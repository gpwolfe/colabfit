"""
Silica_Allegro_15000K ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures)

File notes
----------
Source: vast_ingest/silica_allegro_15000k/silica_allegro_15000k/
        al_out_allegro_v4/{initial_set,iter_*}/.../<hash>/vasprun.xml
The r2SCAN reference labels for an Allegro machine-learned interatomic potential
for silica up to 15000 K. Structures were selected by HYAL active learning (an
Allegro/LAMMPS sampler proposing configurations of alpha-quartz,
beta-cristobalite, coesite, and amorphous silica across melt, high-temperature
melt, melt-quench, and mechanical shear/tension stages; see sampler.ipynb) and
each was labeled with a single-point VASP calculation. Per sampler.ipynb each
label is a PBE pre-convergence followed by the production r2SCAN calculation;
the vaspruns confirm METAGGA=R2SCAN, ENCUT=1000 eV, EDIFF=1e-6 eV, ISMEAR=0,
SIGMA=0.1 eV, Gamma-point sampling, single-point (IBRION=-1, NSW=-1), PAW_PBE
potentials. Energy/forces/stress are read from atoms.calc.results, which
from_ase preserves. A handful of vaspruns (~10, mostly iter_21/iter_23) failed
to write completely and are skipped. The PBE initial-structure-generation MD
(initial_system_generation), the r2SCAN ENCUT-convergence study
(encut_sensitivity_analysis), isolated-atom references (single_atom_energies),
and classical fracture-simulation outputs (silica_fracture_data) are NOT
ingested.
"""

import os
import re
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
loader.config_table = "ndb.colabfit.dev.co_silica_15000k"
loader.config_set_table = "ndb.colabfit.dev.cs_silica_15000k"
loader.dataset_table = "ndb.colabfit.dev.ds_silica_15000k"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_silica_15000k"

DATASET_ID = "DS_p4itrapv72sc_0"
DATASET_NAME = "Silica_Allegro_15000K"
DESCRIPTION = (
    "DFT reference dataset for an Allegro machine-learned interatomic potential "
    "for silica (SiO2) valid up to 15000 K, spanning the high-temperature melt, "
    "melt-quench amorphization, and mechanical-deformation regimes. The "
    "configurations were selected by HYAL active learning - an Allegro/LAMMPS "
    "sampler proposing structures of alpha-quartz, beta-cristobalite, coesite, "
    "and amorphous silica across an initial set and melt, high-temperature "
    "melt, melt-quench, and mechanical shear/tension sampling stages - and each "
    "was then labeled with a single-point VASP calculation (roughly 2780 "
    "successfully labeled configurations with total energies, atomic forces, "
    "and stresses). Calculations used VASP 6.3.2 with the r2SCAN meta-GGA "
    "functional (PAW_PBE potentials), a 1000 eV plane-wave cutoff, an electronic "
    "convergence of 1e-6 eV, Gaussian smearing (ISMEAR=0, SIGMA=0.1 eV), and "
    "Gamma-point Brillouin-zone sampling; each r2SCAN calculation was preceded "
    "by a PBE pre-convergence step. The resulting Allegro potential was used to "
    "study dynamic fracture and energy dissipation in silica glass. "
    "Configuration sets group the data by silica system (quartz, cristobalite, "
    "coesite, amorphous)."
)

# Matching publication: the silica-glass fracture study that trained on and uses
# this dataset (Guren et al. 2026; data availability cites this Zenodo record and
# uses al_out_allegro_v4/iter_23/mlp/model_0). The active-learning method itself
# is described in Cezar et al., npj Comput. Mater. (2025) (in OTHER_LINKS).
PUBLICATION = "https://doi.org/10.48550/arXiv.2605.03457"
DATA_LINK = "https://doi.org/10.5281/zenodo.20053085"
OTHER_LINKS = [
    "https://arxiv.org/abs/2605.03457",
    "https://doi.org/10.1038/s41524-025-01703-5",
]
PUBLICATION_YEAR = "2026"

AUTHORS = [
    "Henrik Andersen Sveinsson",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "silica_allegro_15000k" / "al_out_allegro_v4"

# HYAL active-learning iteration -> sampling stage (see sampler.ipynb).
_STAGE_BY_ITER = {}
for _n in range(0, 6):
    _STAGE_BY_ITER[_n] = "melt"
for _n in range(6, 12):
    _STAGE_BY_ITER[_n] = "melt_high_T"
for _n in range(12, 18):
    _STAGE_BY_ITER[_n] = "melt_quench"
for _n in range(18, 24):
    _STAGE_BY_ITER[_n] = "mechanical"

_ITER_RE = re.compile(r"(?:^|/)iter_(\d+)(?:/|$)")


def _system_family(system: str) -> str:
    """Collapse amorphous_silica_0..4 into a single family; keep crystals as-is."""
    return "amorphous_silica" if system.startswith("amorphous_silica") else system


def _parse_path(vasprun: Path):
    """Return (family, system, stage) from an al_out_allegro_v4 vasprun path."""
    system = vasprun.parent.parent.name  # .../<system>/<hash>/vasprun.xml
    family = _system_family(system)
    text = str(vasprun)
    if "/initial_set/" in text:
        stage = "initial_set"
    else:
        m = _ITER_RE.search(text)
        stage = _STAGE_BY_ITER.get(int(m.group(1)), "unknown") if m else "unknown"
    return family, system, stage


def reader(data_path: Path):
    for vasprun in sorted(data_path.rglob("vasprun.xml")):
        family, system, stage = _parse_path(vasprun)
        tag = vasprun.parent.name[:10]
        try:
            images = list(iread(vasprun, index=":", format="vasp-xml"))
        except Exception as e:
            # ~10 vaspruns (mostly iter_21/iter_23) are truncated and unparseable.
            print(f"Skipping unreadable vasprun {vasprun}: {e}")
            continue
        for i, atoms in enumerate(images):
            if atoms.calc is None or "energy" not in atoms.calc.results:
                print(f"Skipping frame without energy in {vasprun} (frame {i})")
                continue
            atoms.info["_name"] = f"{DATASET_NAME}__{family}__{stage}__{tag}__{i}"
            atoms.info["system"] = system
            atoms.info["stage"] = stage
            yield AtomicConfiguration.from_ase(atoms)


property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])

property_map.set_metadata_field("software", "VASP 6.3.2")
property_map.set_metadata_field("method", "DFT-r2SCAN")
property_map.set_metadata_field(
    "input",
    {
        "functional": "r2SCAN meta-GGA (METAGGA=R2SCAN); PAW_PBE potentials",
        "ENCUT": "1000 eV",
        "EDIFF": "1e-6 eV",
        "ISMEAR": "0 (Gaussian smearing)",
        "SIGMA": "0.1 eV",
        "kpoints": "Gamma-point only (1x1x1)",
        "calculation": "single-point (IBRION=-1, NSW=-1), PBE pre-convergence",
        "description": (
            "VASP 6.3.2 r2SCAN single-point reference labels for HYAL "
            "active-learning configurations. Each label was pre-converged at "
            "PBE then computed with r2SCAN (ENCUT=1000 eV, EDIFF=1e-6 eV)."
        ),
    },
)
property_map.set_metadata_field("system", "system", dynamic=True)
property_map.set_metadata_field("stage", "stage", dynamic=True)

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
    "alpha_quartz": "Alpha-quartz crystalline silica configurations.",
    "beta_cristobalite": "Beta-cristobalite crystalline silica configurations.",
    "coesite": "Coesite crystalline silica configurations.",
    "amorphous_silica": (
        "Amorphous silica configurations (melt-quenched and mechanically "
        "deformed)."
    ),
}

CSS = [
    (
        f"{DATASET_NAME}__{family}__.*",
        None,
        f"{DATASET_NAME}_{family}",
        desc,
        False,
    )
    for family, desc in SUBSETS.items()
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

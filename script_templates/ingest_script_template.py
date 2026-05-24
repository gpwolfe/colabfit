"""
<DatasetName_Split> ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Angstrom
cauchy-stress: stress tensor in eV/Angstrom^3 (periodic structures only)
atomization-energy: atomization energy in eV  # include only if present
adsorption-energy: adsorption energy in eV    # include only if present

File notes
----------
Source: <relative path to data file or directory>
<1-3 sentence summary of dataset: what it covers, scale, DFT method, notable splits>

XYZ/LMDB header keys   # document data fields found during exploration
---------------
energy, forces, stress (periodic only), <other_key>, ...
Per-atom: species, pos, forces
"""

# =============================================================================
# TEMPLATE NOTES
# =============================================================================
# Two usage patterns:
#
# PATTERN A — extxyz / JSON (small-to-medium, single-node):
#   - Credentials + loader at module level (no __main__ guard needed)
#   - gen = reader(DATA_PATH); dm = DataManager(configs=gen, ...)
#   - dm.load_co_po_to_vastdb(loader, batching_ingest=True, ...)
#
# PATTERN B — LMDB / large parallel (hundreds of thousands of structures):
#   - Credentials + loader + all ingest logic inside if __name__ == "__main__":
#   - Define a top-level _worker() function (must be picklable for multiprocessing)
#   - Use dm.load_co_po_to_vastdb_parallel(...) with n_workers, write_batch_size
#   - Track progress via PROGRESS_FILE (jsonl) for resumable ingests
#   - Drop stale config table if no progress file exists (get_session pattern)
#
# This template defaults to PATTERN A. See the PATTERN B section at the
# bottom for the parallel/LMDB variant. Delete whichever pattern you don't use.
# =============================================================================

import os
from pathlib import Path

# For extxyz reading (PATTERN A):
from ase.io import iread

# For JSON reading (uncomment if needed):
# import json
# from ase import Atoms as AseAtoms

# For LMDB reading (PATTERN B — also needs: from logging import getLogger; import json):
# from fairchem.core.datasets import AseDBDataset

from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # atomization_energy_pd,  # uncomment if needed
    # adsorption_energy_pd,   # uncomment if needed
    cauchy_stress_pd,          # remove if molecular (pbc=False)
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import PropertyInfo, PropertyMap

# For PATTERN B parallel ingest only:
# from colabfit.tools.vast.property import Property, PropertyInfo, PropertyMap
# from colabfit.tools.vast.utils import get_session
# from logging import getLogger
# logger = getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# PATTERN A: credentials + loader at module level
# PATTERN B: move everything below into if __name__ == "__main__": block
# -----------------------------------------------------------------------------
access_key = os.getenv("VAST_DB_ACCESS")
access_secret = os.getenv("VAST_DB_SECRET")
endpoint = os.getenv("SPARK_ENDPOINT")

loader = VastDataLoader(
    access_key=access_key,
    access_secret=access_secret,
    endpoint=endpoint,
)
# Short name for table suffix: lowercase, underscores, <= ~20 chars
# e.g. mad15_r2sc_val, odac25_val_full, oc25_train
loader.config_table = "ndb.colabfit.dev.co_<shortname>"
loader.config_set_table = "ndb.colabfit.dev.cs_<shortname>"
loader.dataset_table = "ndb.colabfit.dev.ds_<shortname>"
loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_<shortname>"

# Generate once with:
#   python -c "from colabfit.tools.vast.utils import generate_ds_id; print(generate_ds_id())"
# Then paste and never regenerate.
DATASET_ID = "DS_xxxxxxxxxxxx_0"

DATASET_NAME = "<DatasetName_Split>"

DESCRIPTION = (
    # Open with scientific purpose (what problem, what materials/systems covered).
    # State scale (number of structures/calculations).
    # Name DFT software+version, functional, key convergence settings.
    # Note improvements vs predecessors if applicable.
    # For multi-split datasets, share a common base and vary only the last sentence.
    # Last sentence example: "This is the validation split (~10% of cleaned data)."
    "<description>"
)

PUBLICATION = "https://doi.org/..."
DATA_LINK = "https://..."
OTHER_LINKS = ["https://..."]  # or None
PUBLICATION_YEAR = "20XX"

AUTHORS = [
    "First Last",
    "First M. Last",
]
LICENSE = "CC-BY-4.0"
DOI = None

DATA_PATH = Path(__file__).parent / "<filename.xyz>"
# For LMDB directory: DATA_PATH = Path("<relative/path/to/dir>")

# -----------------------------------------------------------------------------
# CONFIG SETS (optional)
# Only create if there is a meaningful subdivision (multiple data_id values,
# partitions, source types). Do NOT create a CS that spans the whole dataset.
#
# Option 1 — dict-driven (used when subsets come from a known metadata field):
# SUBSET_DESCRIPTIONS = {
#     "subset_key": "Description of this subset, part of the <DatasetName> dataset.",
#     ...
# }
# CSS = [
#     (f"__{subset}__", None, f"{DATASET_NAME}__{subset}", desc, False)
#     for subset, desc in SUBSET_DESCRIPTIONS.items()
# ]
#
# Option 2 — inline list (used when partitions are fixed and small in number):
# CSS = [
#     (
#         f"{DATASET_NAME}__<partition>__.*",  # regex matched against _name
#         None,                                 # label match (None = all)
#         f"{DATASET_NAME}_<PartitionDisplayName>",
#         "Description of this partition.",
#         False,                                # ordered
#     ),
# ]
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# READER
# -----------------------------------------------------------------------------

def reader(fp: Path):
    """Yield AtomicConfiguration objects from fp."""
    # --- extxyz variant ---
    for atoms in iread(fp, format="extxyz"):
        # Extract fields used for naming and config sets
        subset = atoms.info.get("subset", "unknown")
        frame_id = atoms.info.get("frame_id", "")
        # _name must be unique per configuration
        atoms.info["_name"] = f"{DATASET_NAME}__{subset}__{frame_id}"
        yield AtomicConfiguration.from_ase(atoms)

    # --- JSON variant (uncomment and replace extxyz block) ---
    # with open(fp) as f:
    #     data = json.load(f)
    # for record in data:
    #     atoms = AseAtoms(
    #         symbols=record["symbols"],
    #         positions=record["positions"],
    #         cell=record.get("cell"),
    #         pbc=record.get("pbc", False),
    #     )
    #     atoms.info = {
    #         "_name": f"{DATASET_NAME}__{record['id']}",
    #         "energy": record["energy"],
    #         "forces": record["forces"],
    #         # add further keys from record as needed
    #     }
    #     yield AtomicConfiguration.from_ase(atoms)

    # --- LMDB (single shard or directory) variant ---
    # dataset = AseDBDataset({"src": str(fp)})
    # for i in dataset.indices:
    #     atoms = dataset.get_atoms(i)
    #     source = atoms.info.get("source", f"index_{i}")
    #     fid = atoms.info.get("fid", 0)
    #     name_str = source.replace("/", "_") + f"__fid_{fid}"
    #     # Clear atoms.info to avoid passing through large arrays
    #     atoms.info = {
    #         "_name": f"{DATASET_NAME}__{name_str}",
    #         "energy": atoms.get_potential_energy(),
    #         "forces": atoms.get_forces().tolist(),
    #         "source": source,
    #         "fid": fid,
    #     }
    #     yield AtomicConfiguration.from_ase(atoms)


# -----------------------------------------------------------------------------
# PROPERTY MAP
# -----------------------------------------------------------------------------

property_map = PropertyMap([energy_pd, atomic_forces_pd, cauchy_stress_pd])
# Molecular (pbc=False) — remove cauchy_stress_pd:
# property_map = PropertyMap([energy_pd, atomic_forces_pd])

property_map.set_metadata_field("software", "<Software vX.Y.Z>")
# Method format: "DFT-<functional>" e.g. "DFT-r2SCAN", "DFT-PBE+D3", "DFT-RPBE+D3"
property_map.set_metadata_field("method", "DFT-<functional>")
property_map.set_metadata_field(
    "input",
    {
        "functional": "<functional name and spin treatment>",
        # VASP-style keys:
        # "ENCUT": "400 eV",
        # "EDIFF": "1e-5 eV",
        # "kpoints": "...",
        # FHI-aims-style keys:
        # "basis_sets": "FHI-aims tight NAO, species defaults 2020",
        # "kpoint_density": "8 Angstrom^-1 (periodic systems)",
        # "smearing": "Gaussian, 0.05 eV",
        # "scf_energy_convergence": "1e-6 eV",
        # "scf_force_convergence": "1e-4 eV/Angstrom",
        # "scf_density_convergence": "1e-5 e*a0^-3",
        "description": "<any additional convergence/grid/basis details>",
    },
)
# Dynamic fields: value is read from atoms.info at ingest time (not a literal)
property_map.set_metadata_field("subset", "subset", dynamic=True)
property_map.set_metadata_field("frame_id", "frame_id", dynamic=True)

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
# atomization_energy_info = PropertyInfo(
#     property_name="atomization-energy",
#     field="atomization_energy",
#     units="eV",
#     original_file_key="atomization_energy",
#     additional=[("per-atom", {"value": False, "units": None})],
# )
# adsorption_energy_info = PropertyInfo(
#     property_name="adsorption-energy",
#     field="adsorption_energy",
#     units="eV",
#     original_file_key="energy_ads",
#     additional=[("per-atom", {"value": False, "units": None})],
# )

property_map.set_properties([energy_info, force_info, stress_info])
# Molecular: property_map.set_properties([energy_info, force_info])
PROPERTY_MAP = property_map.get_property_map()


# -----------------------------------------------------------------------------
# INGEST — PATTERN A (simple, sequential)
# -----------------------------------------------------------------------------

gen = reader(DATA_PATH)

dm = DataManager(
    prop_defs=[energy_pd, atomic_forces_pd, cauchy_stress_pd],
    configs=gen,
    prop_map=PROPERTY_MAP,
    dataset_id=DATASET_ID,
    standardize_energy=True,
    read_write_batch_size=10000,
    # Use suppress_warnings when a property is absent for a subset of configs
    # (e.g. stress only present for periodic structures in a mixed dataset):
    suppress_warnings={"cauchy-stress"},
)

dm.load_co_po_to_vastdb(loader, batching_ingest=True, check_existing=False)

# Config sets — omit entirely if no meaningful subdivisions exist
# Option 1 (dict-driven):
# dm.create_configuration_sets(loader, CSS)
#
# Option 2 (inline name_label_match):
# dm.create_configuration_sets(
#     loader=loader,
#     name_label_match=[
#         (
#             f"{DATASET_NAME}__<partition>__.*",
#             None,
#             f"{DATASET_NAME}_<DisplayName>",
#             "Description of this partition.",
#             False,
#         ),
#     ],
# )

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
    date_requested="YYYY-MM-DD",  # date the dataset was requested for ingestion
)


# =============================================================================
# PATTERN B — LMDB / large parallel ingest
# Delete everything from "INGEST — PATTERN A" above and use this block instead.
# Move credentials + loader into the __main__ block below.
# =============================================================================

# CHUNK_SIZE = 5_000
# PROGRESS_FILE = Path(f"progress_{DATASET_ID}.jsonl")
#
#
# def _dataset_worker(args):
#     """
#     Process a chunk of indices from one .aselmdb shard.
#     Returns (chunk_id, list[(co_row_dict, po_row_dict)]).
#     Top-level so multiprocessing spawn can pickle it.
#     """
#     (
#         shard_file,
#         indices,
#         dataset_name,
#         prop_map,
#         prop_defs,
#         dataset_id,
#         standardize_energy,
#         suppress_warnings,
#     ) = args
#     dataset = AseDBDataset({"src": str(shard_file)})
#     results = []
#     for i in indices:
#         atoms = dataset.get_atoms(i)
#         source = atoms.info.get("source", f"index_{i}")
#         fid = atoms.info.get("fid", 0)
#         name_str = source.replace("/", "_") + f"__fid_{fid}"
#         atoms.info = {
#             "_name": f"{dataset_name}__{name_str}",
#             "energy": atoms.get_potential_energy(),
#             "forces": atoms.get_forces().tolist(),
#             "source": source,
#             "fid": fid,
#         }
#         config = AtomicConfiguration.from_ase(atoms)
#         config.set_dataset_id(dataset_id)
#         prop = Property.from_definition(
#             definitions=prop_defs,
#             configuration=config,
#             property_map=prop_map,
#             standardize_energy=standardize_energy,
#             suppress_warnings=suppress_warnings,
#         )
#         results.append((config.row_dict, prop.row_dict))
#     chunk_id = {"file": str(shard_file), "start": int(indices[0]) if indices else 0}
#     return chunk_id, results
#
#
# if __name__ == "__main__":
#     load_dotenv()
#     access_key = os.getenv("VAST_DB_ACCESS")
#     access_secret = os.getenv("VAST_DB_SECRET")
#     endpoint = os.getenv("SPARK_ENDPOINT")
#
#     loader = VastDataLoader(
#         access_key=access_key,
#         access_secret=access_secret,
#         endpoint=endpoint,
#     )
#     loader.config_table = "ndb.colabfit.dev.co_<shortname>"
#     loader.config_set_table = "ndb.colabfit.dev.cs_<shortname>"
#     loader.dataset_table = "ndb.colabfit.dev.ds_<shortname>"
#     loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_<shortname>"
#
#     if PROGRESS_FILE.exists():
#         logger.info(
#             f"Progress file found at {PROGRESS_FILE}; resuming ingest, skipping table drop."
#         )
#     else:
#         with get_session().transaction() as tx:
#             _, b, s, t = loader.config_table.split(".")
#             sch = tx.bucket(b).schema(s)
#             if sch.table(t, fail_if_missing=False) is not None:
#                 logger.info(f"Dropping stale config table {loader.config_table}")
#                 sch.table(t).drop()
#
#     # --- Build property map (same as PATTERN A, paste here) ---
#     property_map = PropertyMap([energy_pd, atomic_forces_pd])
#     property_map.set_metadata_field("software", "<Software vX.Y.Z>")
#     property_map.set_metadata_field("method", "DFT-<functional>")
#     property_map.set_metadata_field("input", {...})
#     energy_info = PropertyInfo(
#         property_name="energy", field="energy", units="eV",
#         original_file_key="energy",
#         additional=[("per-atom", {"value": False, "units": None})],
#     )
#     force_info = PropertyInfo(
#         property_name="atomic-forces", field="forces", units="eV/angstrom",
#         original_file_key="forces",
#     )
#     property_map.set_properties([energy_info, force_info])
#     PROPERTY_MAP = property_map.get_property_map()
#     logger.info(PROPERTY_MAP)
#
#     prop_defs = [energy_pd, atomic_forces_pd]
#
#     # --- Resume logic ---
#     completed_chunks = set()
#     if PROGRESS_FILE.exists():
#         with open(PROGRESS_FILE) as pf:
#             for line in pf:
#                 line = line.strip()
#                 if line:
#                     import json
#                     rec = json.loads(line)
#                     completed_chunks.add((rec["file"], rec["start"]))
#         logger.info(f"Resuming: {len(completed_chunks)} chunks already completed")
#
#     # --- Build worker args ---
#     shard_files = sorted(DATA_PATH.glob("*.aselmdb"))
#     if not shard_files:
#         raise FileNotFoundError(f"No .aselmdb shards found in {DATA_PATH}")
#     worker_args = []
#     for shard_file in shard_files:
#         shard = AseDBDataset({"src": str(shard_file)})
#         indices = list(shard.indices)
#         for i in range(0, len(indices), CHUNK_SIZE):
#             chunk_indices = indices[i : i + CHUNK_SIZE]
#             if completed_chunks and (str(shard_file), chunk_indices[0]) in completed_chunks:
#                 continue
#             worker_args.append((
#                 shard_file, chunk_indices, DATASET_NAME,
#                 PROPERTY_MAP, prop_defs, DATASET_ID, True, set(),
#             ))
#     logger.info(f"Worker chunks remaining: {len(worker_args)}")
#
#     n_workers = int(os.getenv("COLABFIT_WORKERS", os.getenv("SLURM_CPUS_PER_TASK", "8")))
#     dm = DataManager(
#         prop_defs=prop_defs,
#         prop_map=PROPERTY_MAP,
#         dataset_id=DATASET_ID,
#         standardize_energy=True,
#         suppress_warnings=set(),
#     )
#     dm.load_co_po_to_vastdb_parallel(
#         loader=loader,
#         worker_fn=_dataset_worker,
#         worker_args=worker_args,
#         n_workers=n_workers,
#         write_batch_size=100_000,
#         check_existing=False,
#         progress_file=PROGRESS_FILE,
#     )
#
#     # Config sets (same options as PATTERN A)
#     # dm.create_configuration_sets(loader, CSS)
#
#     dm.create_dataset(
#         loader=loader,
#         name=DATASET_NAME,
#         authors=AUTHORS,
#         publication_link=PUBLICATION,
#         data_link=DATA_LINK,
#         other_links=OTHER_LINKS,
#         description=DESCRIPTION,
#         data_license=LICENSE,
#         publication_year=PUBLICATION_YEAR,
#         equilibrium=False,
#         date_requested="YYYY-MM-DD",
#     )

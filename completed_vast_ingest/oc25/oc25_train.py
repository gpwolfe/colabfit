"""
Open_Catalyst_2025_OC25_Train ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Å

File notes
----------
Source: vast_ingest/oc25/train/ (20 .aselmdb shards)
OC25 is the largest solid-liquid interface dataset, containing 7,801,261
single-point DFT calculations of catalyst/solvent/ion/adsorbate structures
spanning 88 elements, 8 solvents, 9 ionic species, and adsorbates from OC20
plus reactive intermediates. Structures are highly off-equilibrium, sampled
from short AIMD (10-50 steps, 1000K, NVT) or short DFT relaxations (5 ionic
steps). The training split (7,395,512 structures) is filtered to total force
drift < 1 eV/Å. DFT: VASP 6.3.2, non-spin-polarized RPBE+D3 (zero damping),
ENCUT=400 eV, EDIFF=1e-4 eV, k-point reciprocal density=40, dipole correction
in z.
"""

import json
import os
from logging import getLogger
from pathlib import Path

from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import Property, PropertyInfo, PropertyMap
from colabfit.tools.vast.utils import get_session
from dotenv import load_dotenv
from fairchem.core.datasets import AseDBDataset

logger = getLogger(__name__)

DATASET_ID = "DS_fanupene3rn7_0"
DATASET_NAME = "Open_Catalyst_2025_OC25_Train"
DESCRIPTION = (
    "The training split of the Open Catalyst 2025 (OC25) dataset for solid-liquid "
    "interfaces. OC25 consists of single-point DFT calculations of "
    "catalyst/solvent/ion/adsorbate structures, covering 88 elements, 8 solvents "
    "(water, methanol, CCl4, DMSO, benzene, hexane, THF, diethyl ether), 9 ionic "
    "species (Cs+, OH-, Li+, SO4^2-, Ca^2+, [Me4N]+, HCO3-, H+, F-), and adsorbates "
    "from the OC20 set plus reactive intermediates. Surfaces are derived from 39,821 "
    "Materials Project bulk structures with miller indices <= 3. Structures are highly "
    "off-equilibrium, sampled from short ab initio molecular dynamics simulations "
    "(10-50 steps, 1000K, NVT) or short DFT relaxations (5 ionic steps). The training "
    "split contains ~7.4 million structures filtered to total force drift < 1 eV/Å. All "
    "DFT calculations used VASP 6.3.2 with the non-spin-polarized RPBE functional "
    "supplemented with D3 dispersion correction (zero damping), plane wave cutoff "
    "400 eV, EDIFF=1e-4 eV, k-point reciprocal density of 40, and a dipole correction "
    "in the z-direction."
)
PUBLICATION = "https://doi.org/10.48550/arXiv.2509.17862"
DATA_LINK = "https://huggingface.co/facebook/OC25"
OTHER_LINKS = ["https://github.com/facebookresearch/fairchem"]
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Sushree Jagriti Sahoo",
    "Mikael Maroschin",
    "Daniel S. Levine",
    "Zachary Ulissi",
    "C. Lawrence Zitnick",
    "Joel B Varley",
    "Joseph A. Gauthier",
    "Nitish Govindarajan",
    "Muhammed Shuaibi",
]
LICENSE = "CC-BY-4.0"
DOI = None
DATA_PATH = Path("oc25/train")
CHUNK_SIZE = 2_000
PROGRESS_FILE = Path(f"progress_{DATASET_ID}.jsonl")


def _oc25_worker(args):
    """
    Process a chunk of indices from one .aselmdb shard.
    Returns (chunk_id, list[(co_row_dict, po_row_dict)]).
    Top-level so multiprocessing spawn can pickle it.
    """
    (
        shard_file,
        indices,
        dataset_name,
        prop_map,
        prop_defs,
        dataset_id,
        standardize_energy,
        suppress_warnings,
    ) = args
    dataset = AseDBDataset({"src": str(shard_file)})
    results = []
    for i in indices:
        atoms = dataset.get_atoms(i)
        energy = atoms.get_potential_energy()
        forces = atoms.get_forces().tolist()
        source = atoms.info.get("source", f"index_{i}")
        fid = atoms.info.get("fid", 0)
        total_drift = atoms.info.get("total_drift")

        name_str = source.replace("/", "_") + f"__fid_{fid}"
        keep = {
            "_name": f"{dataset_name}__{name_str}",
            "energy": energy,
            "forces": forces,
            "source": source,
            "fid": fid,
        }
        if total_drift is not None:
            keep["total_drift"] = total_drift
        atoms.info = keep

        config = AtomicConfiguration.from_ase(atoms)
        config.set_dataset_id(dataset_id)
        prop = Property.from_definition(
            definitions=prop_defs,
            configuration=config,
            property_map=prop_map,
            standardize_energy=standardize_energy,
            suppress_warnings=suppress_warnings,
        )
        results.append((config.row_dict, prop.row_dict))
    chunk_id = {"file": str(shard_file), "start": int(indices[0]) if indices else 0}
    return chunk_id, results


def reader(data_path: Path):
    shard_files = sorted(data_path.glob("*.aselmdb"))
    for shard_file in shard_files:
        dataset = AseDBDataset({"src": str(shard_file)})
        for i in dataset.indices:
            atoms = dataset.get_atoms(i)
            energy = atoms.get_potential_energy()
            forces = atoms.get_forces().tolist()
            source = atoms.info.get("source", f"index_{i}")
            fid = atoms.info.get("fid", 0)
            total_drift = atoms.info.get("total_drift")

            name_str = source.replace("/", "_") + f"__fid_{fid}"
            keep = {
                "_name": f"{DATASET_NAME}__{name_str}",
                "energy": energy,
                "forces": forces,
                "source": source,
                "fid": fid,
            }
            if total_drift is not None:
                keep["total_drift"] = total_drift
            atoms.info = keep
            yield AtomicConfiguration.from_ase(atoms)


if __name__ == "__main__":
    load_dotenv()
    access_key = os.getenv("VAST_DB_ACCESS")
    access_secret = os.getenv("VAST_DB_SECRET")
    endpoint = os.getenv("SPARK_ENDPOINT")

    loader = VastDataLoader(
        access_key=access_key,
        access_secret=access_secret,
        endpoint=endpoint,
        limit_rows_per_sub_split=250000,
    )
    loader.config_table = "ndb.colabfit.dev.co_oc25_trn"
    loader.config_set_table = "ndb.colabfit.dev.cs_oc25_trn"
    loader.dataset_table = "ndb.colabfit.dev.ds_oc25_trn"
    loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_oc25_trn"

    if PROGRESS_FILE.exists():
        logger.info(
            f"Progress file found at {PROGRESS_FILE}; resuming ingest, skipping table drop."
        )
    else:
        with get_session().transaction() as tx:
            _, b, s, t = loader.config_table.split(".")
            sch = tx.bucket(b).schema(s)
            if sch.table(t, fail_if_missing=False) is not None:
                logger.info(
                    f"Config table exists. Dropping config table {loader.config_table}"
                )
                sch.table(t).drop()

    property_map = PropertyMap([energy_pd, atomic_forces_pd])
    property_map.set_metadata_field("software", "VASP 6.3.2")
    property_map.set_metadata_field("method", "DFT-rPBE+D3")
    property_map.set_metadata_field(
        "input",
        {
            "functional": "RPBE (non-spin-polarized)",
            "dispersion": "D3 with zero damping",
            "ENCUT": "400 eV",
            "EDIFF": "1e-4 eV",
            "kpoints": "reciprocal density of 40",
            "dipole_correction": "z-direction",
            "sampling": (
                "Short AIMD (10-50 steps, 1000K, NVT) or DFT relaxations (5 ionic steps); "
                "filtered to total force drift < 1 eV/Å"
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
    force_info = PropertyInfo(
        property_name="atomic-forces",
        field="forces",
        units="eV/angstrom",
        original_file_key="forces",
    )
    property_map.set_properties([energy_info, force_info])
    PROPERTY_MAP = property_map.get_property_map()
    logger.info(PROPERTY_MAP)

    prop_defs = [energy_pd, atomic_forces_pd]

    completed_chunks = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as pf:
            for line in pf:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    completed_chunks.add((rec["file"], rec["start"]))
        logger.info(f"Resuming: {len(completed_chunks)} chunks already completed")

    shard_files = sorted(DATA_PATH.glob("*.aselmdb"))
    if not shard_files:
        raise FileNotFoundError(f"No .aselmdb shards found in {DATA_PATH}")

    worker_args = []
    for shard_file in shard_files:
        shard = AseDBDataset({"src": str(shard_file)})
        indices = list(shard.indices)
        for i in range(0, len(indices), CHUNK_SIZE):
            chunk_indices = indices[i : i + CHUNK_SIZE]
            if (
                completed_chunks
                and (str(shard_file), chunk_indices[0]) in completed_chunks
            ):
                continue
            worker_args.append(
                (
                    shard_file,
                    chunk_indices,
                    DATASET_NAME,
                    PROPERTY_MAP,
                    prop_defs,
                    DATASET_ID,
                    True,
                    set(),
                )
            )
    logger.info(f"Worker chunks remaining: {len(worker_args)}")

    n_workers = int(
        os.getenv("COLABFIT_WORKERS", os.getenv("SLURM_CPUS_PER_TASK", "4"))
    )
    dm = DataManager(
        prop_defs=prop_defs,
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        suppress_warnings=set(),
    )
    dm.load_co_po_to_vastdb_parallel(
        loader=loader,
        worker_fn=_oc25_worker,
        worker_args=worker_args,
        n_workers=n_workers,
        write_batch_size=20_000,
        check_existing=False,
        progress_file=PROGRESS_FILE,
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
        date_requested="2026-05-18",
    )

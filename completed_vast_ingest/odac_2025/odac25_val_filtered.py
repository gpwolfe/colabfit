"""
Open_Direct_Air_Capture_ODAC2025_Val_Filtered ingest script for ColabFit VAST database.

Properties
----------
energy: total DFT energy in eV
atomic-forces: forces in eV/Å
adsorption-energy: adsorption energy in eV (mof_plus_adsorbate partition only)

File notes
----------
Source: vast_ingest/odac_2025/val/
ODAC25 is the largest high-quality DFT dataset for Direct Air Capture, containing
over 15,000 MOFs with 4 adsorbates (CO2, H2O, N2, O2). Three partitions:
(1) mof_plus_adsorbate: full DFT relaxations of adsorbates on MOFs
(2) mof: re-relaxations of empty MOFs
(3) gcmc: DFT single points from Grand Canonical Monte Carlo simulations
The Filtered dataset excludes MOFs deemed problematic by Jin et al. (2025).
"""

import json
import os
from logging import getLogger
from pathlib import Path

from colabfit.tools.property_definitions import (
    adsorption_energy_pd,
    atomic_forces_pd,
    energy_pd,
)
from colabfit.tools.vast.configuration import AtomicConfiguration
from colabfit.tools.vast.database import DataManager, VastDataLoader
from colabfit.tools.vast.property import Property, PropertyInfo, PropertyMap
from colabfit.tools.vast.utils import get_session
from dotenv import load_dotenv
from fairchem.core.datasets import AseDBDataset

logger = getLogger(__name__)

DATASET_ID = "DS_efdsbceoo5gc_0"
DATASET_NAME = "Open_Direct_Air_Capture_ODAC2025_Val_Filtered"
DESCRIPTION = (
    "This is the filtered validation split of ODAC25. Open Direct Air Capture 2025 (ODAC25) "
    "is the largest high-quality DFT dataset "
    "for Direct Air Capture, containing over 15,000 Metal-Organic Frameworks (MOFs), "
    "including experimental, defective, synthetic, and amine-functionalized MOFs, with "
    "4 adsorbates: CO2, H2O, N2, and O2. ODAC25 significantly improves upon ODAC23 by "
    "adding functionalized MOFs, new adsorbates (N2 and O2), higher k-point convergence, "
    "and re-relaxations of empty MOFs. The dataset contains three partitions: "
    "(1) mof_plus_adsorbate includes full DFT relaxations of different adsorbates on "
    "various MOFs; (2) mof includes re-relaxations of empty MOFs; (3) gcmc includes DFT "
    "single points of configurations derived from Grand Canonical Monte Carlo (GCMC) "
    "simulations. MOFs deemed problematic by "
    "Jin et al. (2025) have been excluded (see https://zenodo.org/records/14802658)."
)
PUBLICATION = "https://doi.org/10.48550/arXiv.2508.03162"
DATA_LINK = "https://huggingface.co/facebook/ODAC25"
OTHER_LINKS = ["https://open-dac.github.io/"]
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Anuroop Sriram",
    "Logan M. Brabson",
    "Xiaohan Yu",
    "Sihoon Choi",
    "Kareem Abdelmaqsoud",
    "Elias Moubarak",
    "Pim de Haan",
    "Sindy Löwe",
    "Johann Brehmer",
    "John R. Kitchin",
    "Max Welling",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
    "Andrew J. Medford",
    "David S. Sholl",
]
LICENSE = "CC-BY-4.0"
DOI = None
DATA_PATH = Path("odac25_filtered_val/val")
PARTITIONS = ["mof_plus_adsorbate", "mof", "gcmc"]
CHUNK_SIZE = 5_000
PROGRESS_FILE = Path(f"progress_{DATASET_ID}.jsonl")


def _odac25_worker(args):
    """
    Process a chunk of indices from one .aselmdb shard.
    Returns (chunk_id, list[(co_row_dict, po_row_dict)]).
    Must be a top-level function so multiprocessing spawn can pickle it.
    """
    (
        part_file,
        indices,
        dataset_name,
        partition_name,
        prop_map,
        prop_defs,
        dataset_id,
        standardize_energy,
        suppress_warnings,
    ) = args
    dataset = AseDBDataset({"src": str(part_file)})
    results = []
    for i in indices:
        atoms = dataset.get_atoms(i)
        name_str = atoms.info.get("name", f"index_{i}")
        energy_ads = atoms.info.get("energy_ads")
        energy_ads_corr = atoms.info.get("energy_ads_corrected")
        energy_mof = atoms.info.get("energy_mof")
        keep = {
            "_name": f"{dataset_name}__{partition_name}__{name_str}",
            "energy": atoms.info.get("energy"),
            "forces": atoms.get_forces().tolist(),
            "mof_name": atoms.info.get("mof_name"),
            "mof_type": atoms.info.get("mof_type"),
            "nco2": atoms.info.get("nco2"),
            "nh2o": atoms.info.get("nh2o"),
            "nn2": atoms.info.get("nn2"),
            "no2": atoms.info.get("no2"),
            "nads": atoms.info.get("nads"),
            "sid": atoms.info.get("sid"),
            "fid": atoms.info.get("fid"),
        }
        if energy_ads is not None:
            keep["adsorption_energy"] = energy_ads
        if energy_ads_corr is not None:
            keep["energy_ads_corrected"] = energy_ads_corr
        if energy_mof is not None:
            keep["energy_mof"] = energy_mof
        atoms.info = {k: v for k, v in keep.items() if v is not None}
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
    chunk_id = {"file": str(part_file), "start": int(indices[0]) if indices else 0}
    return chunk_id, results


def reader(data_path: Path):
    for partition in PARTITIONS:
        part_path = data_path / partition
        if not part_path.exists():
            logger.info(f"Partition path not found, skipping: {part_path}")
            continue
        dataset = AseDBDataset({"src": str(part_path)})
        for i in dataset.indices:
            atoms = dataset.get_atoms(i)
            name_str = atoms.info.get("name", f"index_{i}")
            energy_ads = atoms.info.get("energy_ads")
            energy_ads_corr = atoms.info.get("energy_ads_corrected")
            energy_mof = atoms.info.get("energy_mof")
            keep = {
                "_name": f"{DATASET_NAME}__{partition}__{name_str}",
                "energy": atoms.info.get("energy"),
                "forces": atoms.get_forces().tolist(),
                "mof_name": atoms.info.get("mof_name"),
                "mof_type": atoms.info.get("mof_type"),
                "nco2": atoms.info.get("nco2"),
                "nh2o": atoms.info.get("nh2o"),
                "nn2": atoms.info.get("nn2"),
                "no2": atoms.info.get("no2"),
                "nads": atoms.info.get("nads"),
                "sid": atoms.info.get("sid"),
                "fid": atoms.info.get("fid"),
            }
            if energy_ads is not None:
                keep["adsorption_energy"] = energy_ads
            if energy_ads_corr is not None:
                keep["energy_ads_corrected"] = energy_ads_corr
            if energy_mof is not None:
                keep["energy_mof"] = energy_mof
            atoms.info = {k: v for k, v in keep.items() if v is not None}
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
    )
    loader.config_table = "ndb.colabfit.dev.co_odac25_val_filt"
    loader.config_set_table = "ndb.colabfit.dev.cs_odac25_val_filt"
    loader.dataset_table = "ndb.colabfit.dev.ds_odac25_val_filt"
    loader.co_cs_map_table = "ndb.colabfit.dev.cs_co_map_odac25_val_filt"

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

    property_map = PropertyMap([energy_pd, atomic_forces_pd, adsorption_energy_pd])
    property_map.set_metadata_field("software", "VASP 6.3")
    property_map.set_metadata_field("method", "DFT-PBE+D3")
    property_map.set_metadata_field(
        "input",
        {
            "GGA": "PE",
            "ENCUT": "600 eV",
            "EDIFF": "1e-5 eV",
            "EDIFFG": "-0.05 eV/angstrom",
            "ISMEAR": "0",
            "SIGMA": "0.2 eV",
            "ISPIN": "2",
            "IVDW": "12 (D3 dispersion with Becke-Johnson damping)",
            "PREC": "Accurate",
            "ALGO": "NORMAL",
            "ISIF": "2 for MOF+adsorbate (fixed cell), 3 for bare MOF (cell relaxation)",
            "ISYM": "0",
            "NSW": "2000",
            "IBRION": "2",
            "KPOINTS": "Gamma-centered, ceil(40/a) x ceil(40/b) x ceil(40/c)",
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
    ads_energy_info = PropertyInfo(
        property_name="adsorption-energy",
        field="adsorption_energy",
        units="eV",
        original_file_key="energy_ads",
        additional=[("per-atom", {"value": False, "units": None})],
    )
    property_map.set_properties([energy_info, force_info, ads_energy_info])
    PROPERTY_MAP = property_map.get_property_map()
    logger.info(PROPERTY_MAP)

    prop_defs = [energy_pd, atomic_forces_pd, adsorption_energy_pd]

    completed_chunks = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as pf:
            for line in pf:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    completed_chunks.add((rec["file"], rec["start"]))
        logger.info(f"Resuming: {len(completed_chunks)} chunks already completed")

    worker_args = []
    for partition in PARTITIONS:
        part_dir = DATA_PATH / partition
        if not part_dir.exists():
            logger.info(f"Partition path not found, skipping: {part_dir}")
            continue
        part_files = sorted(part_dir.glob("*.aselmdb"))
        if not part_files:
            logger.warning(f"No .aselmdb shards found in {part_dir}")
            continue
        for part_file in part_files:
            shard = AseDBDataset({"src": str(part_file)})
            indices = list(shard.indices)
            for i in range(0, len(indices), CHUNK_SIZE):
                chunk_indices = indices[i : i + CHUNK_SIZE]
                if (
                    completed_chunks
                    and (str(part_file), chunk_indices[0]) in completed_chunks
                ):
                    continue
                worker_args.append(
                    (
                        part_file,
                        chunk_indices,
                        DATASET_NAME,
                        partition,
                        PROPERTY_MAP,
                        prop_defs,
                        DATASET_ID,
                        True,
                        {"adsorption-energy"},
                    )
                )
    logger.info(f"Worker chunks remaining: {len(worker_args)}")

    n_workers = int(
        os.getenv("COLABFIT_WORKERS", os.getenv("SLURM_CPUS_PER_TASK", "8"))
    )
    dm = DataManager(
        prop_defs=prop_defs,
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        suppress_warnings={"adsorption-energy"},
    )
    dm.load_co_po_to_vastdb_parallel(
        loader=loader,
        worker_fn=_odac25_worker,
        worker_args=worker_args,
        n_workers=n_workers,
        write_batch_size=100_000,
        check_existing=False,
        progress_file=PROGRESS_FILE,
    )

    dm.create_configuration_sets(
        loader=loader,
        name_label_match=[
            (
                f"{DATASET_NAME}__mof_plus_adsorbate__.*",
                None,
                f"{DATASET_NAME}_mof_plus_adsorbate",
                "Full DFT relaxations of different adsorbates on various MOFs.",
                False,
            ),
            (
                f"{DATASET_NAME}__mof__.*",
                None,
                f"{DATASET_NAME}_mof",
                "Re-relaxations of empty MOFs.",
                False,
            ),
            (
                f"{DATASET_NAME}__gcmc__.*",
                None,
                f"{DATASET_NAME}_gcmc",
                "DFT single points of configurations derived from GCMC simulations.",
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
        date_requested="2026-04-14",
    )

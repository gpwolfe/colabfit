import logging
import sys
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from pickle import dump
from time import time

from colabfit.tools.vast.configuration import AtomicConfiguration
from fairchem.core.datasets import AseDBDataset

logger = logging.getLogger(f"{__name__}.hasher")
logger.setLevel("INFO")

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/omol/train"  # noqa
)
DATASET_NAME = "OMol25_train"
DATASET_ID = "DS_bzcf331ql8ji_0"
DESCRIPTION = "The full-size training set from OMol25. From the dataset creator: OMol25 represents the largest high quality molecular DFT dataset spanning biomolecules, metal complexes, electrolytes, and community datasets. OMol25 was generated at the ω B97M-V/def2-TZVPD level of theory."  # noqa
DOI = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Daniel S. Levine",
    "Muhammed Shuaibi",
    "Evan Walter Clark Spotte-Smith",
    "Michael G. Taylor",
    "Muhammad R. Hasyim",
    "Kyle Michel",
    "Ilyes Batatia",
    "Gábor Csányi",
    "Misko Dzamba",
    "Peter Eastman",
    "Nathan C. Frey",
    "Xiang Fu",
    "Vahe Gharakhanyan",
    "Aditi S. Krishnapriyan",
    "Joshua A. Rackers",
    "Sanjeev Raja",
    "Ammar Rizvi",
    "Andrew S. Rosen",
    "Zachary Ulissi",
    "Santiago Vargas",
    "C. Lawrence Zitnick",
    "Samuel M. Blau",
    "Brandon M. Wood",
]
LICENSE = "CC-BY-4.0"
PUBLICATION = "https://doi.org/10.48550/arXiv.2505.08762"
DATA_LINK = "https://huggingface.co/facebook/OMol25"


def batched(configs, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    configs = iter(configs)
    while True:
        batch = list(islice(configs, n))
        if len(batch) == 0:
            break
        yield batch


def reader(fp):
    db = AseDBDataset({"src": str(fp)})
    for ix in db.indices:
        atoms = db.get_atoms(ix)
        atoms.info["_name"] = f"{DATASET_NAME}_{fp.stem}_{ix}"
        yield AtomicConfiguration.from_ase(atoms)


def process_batch(fp: Path):
    start = time()
    configs = []
    file_ix = 0
    output_dir = Path("omol_train_pickles")
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Processing file: {fp}")
    configs = batched(reader(fp), 10000)
    for ix, batch in enumerate(configs):
        output_path = output_dir / f"{DATASET_NAME}_{fp.stem}_{file_ix}_{ix}.pkl"
        with output_path.open("wb") as f:
            dump(batch, f)
    logger.info("completed")
    logger.info(f"Processed {fp} in {time() - start:.2f} seconds")


def mp_process(fp):
    files = sorted(list(fp.rglob("*.aselmdb")))
    p = Pool(24)
    logger.info("mapping")
    p.map(process_batch, files)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python <script_name> <filepath>")
        sys.exit(1)

    filepath = Path(sys.argv[1])
    if not filepath.exists():
        print(f"File {filepath} does not exist.")
        sys.exit(1)
    mp_process(filepath)

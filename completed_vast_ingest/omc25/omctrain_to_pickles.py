import logging
from multiprocessing import Pool
from pathlib import Path
from pickle import dump
from colabfit.tools.vast.database import batched
from colabfit.tools.vast.configuration import AtomicConfiguration
from dotenv import load_dotenv
from fairchem.core.datasets import AseDBDataset
from tqdm import tqdm


load_dotenv()
logger = logging.getLogger(__name__)


DATASET_FP = Path("omc_train_250802/train")
DATASET_ID = "DS_uv793lz7dc0h_0"
DATASET_NAME = "Open_Molecular_Crystals_2025_OMC25_train"
DESCRIPTION = "The training split of OMC25. Open Molecular Crystals 2025 (OMC25) is a molecular crystal dataset produced by Meta. The OE62 dataset was used as a source for sampling molecules; crystals were generated with Genarris 3.0; from these, relaxation trajectories were generated and sampled to create the final dataset. See the publication for details."  # noqa: E501
PUBLICATION = "https://doi.org/10.48550/arXiv.2508.02651"
DATA_LINK = "https://huggingface.co/facebook/OMC25"
OTHER_LINKS = None
PUBLICATION_YEAR = "2025"
AUTHORS = [
    "Vahe Gharakhanyan",
    "Luis Barroso-Luque",
    "Yi Yang",
    "Muhammed Shuaibi",
    "Kyle Michel",
    "Daniel S. Levine",
    "Misko Dzamba",
    "Xiang Fu",
    "Meng Gao",
    "Xingyu Liu",
    "Haoran Ni",
    "Keian Noori",
    "Brandon M. Wood",
    "Matt Uyttendaele",
    "Arman Boromand",
    "C. Lawrence Zitnick",
    "Noa Marom",
    "Zachary W. Ulissi",
    "Anuroop Sriram",
]
LICENSE = "CC-BY-4.0"
DOI = None


def reader(indices):
    dataset = AseDBDataset({"src": str(DATASET_FP)})
    logger.info(f"indices: {indices[0]}-{indices[-1]}")
    file_ix = indices[0]
    file_path = Path(f"pickles2/{DATASET_NAME}_index_{file_ix}.pkl")
    if file_path.exists():
        logger.info(f"File {file_path} already exists, skipping.")
        return
    configs = []
    for i in tqdm(indices):
        atoms = dataset.get_atoms(i)
        atoms.info["cauchy_stress"] = atoms.get_stress(voigt=False)
        atoms.info["_name"] = f"{DATASET_NAME}_index_{i}"
        c = AtomicConfiguration.from_ase(atoms)
        configs.append(c)
    with open(file_path, "wb") as f:
        dump(configs, f)


def to_pickle():
    pickle_dir = Path("pickles2")
    pickle_dir.mkdir(exist_ok=True)
    dataset = AseDBDataset({"src": str(DATASET_FP)})
    indices = list(dataset.indices)

    keys = [list(k) for k in batched(indices, 10000)]

    keys = [
        k
        for k in keys
        if not Path(f"pickles2/{DATASET_NAME}_index_{k[0]}.pkl").exists()
    ]
    keys = [k for k in keys if k[0] in [9460000]]
    logger.info(f"Number of key batches to process: {len(keys)}")
    # p = Pool(8)
    # results = p.map(reader, keys)
    for k in keys:
        reader(k)
    # return results


if __name__ == "__main__":
    to_pickle()

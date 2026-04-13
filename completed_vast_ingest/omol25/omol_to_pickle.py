import logging
import pickle
import sys
from pathlib import Path

from colabfit.tools.vast.configuration import AtomicConfiguration
from fairchem.core.datasets import AseDBDataset

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


FILE_SIZE = 100_000
dataset_info = [
    {"name": "OMol25_val", "path": "val"},
    {"name": "OMol25_train_4M", "path": "train_4M"},
    {"name": "OMol25_train_neutral", "path": "neutral_train"},
    {"name": "OMol25_test", "path": "test"},
    {"name": "OMol25_train_all", "path": "train"},
]


def reader(ds_path, dataset_name):
    dataset = AseDBDataset({"src": ds_path})
    for i, row in enumerate(dataset):
        atoms = dataset.get_atoms(i)
        info = {
            "data_id": atoms.info["data_id"],
            "_name": f"{dataset_name}_{atoms.info['source']}",
        }
        assert "/" in info["_name"], f"Expected '/' in name, got {info['_name']}"
        atoms.info = info
        c = AtomicConfiguration.from_ase(atoms)
        yield c


def find_last_file_number(output_path):
    """Find the highest numbered pickle file in the output directory."""
    if not output_path.exists():
        return -1

    max_num = -1
    for file_path in output_path.glob("co_*.pkl"):
        try:
            num_str = file_path.stem.split("_")[1]
            num = int(num_str)
            max_num = max(max_num, num)
        except (IndexError, ValueError):
            continue

    return max_num


def pickle_omol(dataset_path, output_path, dataset_name):
    if not output_path.exists():
        output_path.mkdir(parents=True)

    # Find where to resume from
    last_file_num = find_last_file_number(output_path)

    if last_file_num >= 0:
        # If we found existing files, start from the next file number
        file_count = last_file_num + 1
        start_index = file_count * FILE_SIZE  # This is the key fix
    else:
        # Starting fresh
        file_count = 0
        start_index = 0

    logger.info(f"Processing dataset: {dataset_name}")
    logger.info(f"Dataset path: {dataset_path}")
    logger.info(f"Output path: {output_path}")

    if last_file_num >= 0:
        logger.info(f"Found existing files up to co_{last_file_num}.pkl")
        logger.info(f"Resuming from index {start_index} (file co_{file_count}.pkl)")
    else:
        logger.info("Starting from the beginning")

    confs = []
    for i, conf in enumerate(reader(dataset_path, dataset_name)):
        # Skip configurations that have already been processed
        if i < start_index:
            continue

        confs.append(conf)

        # Adjust the condition to account for the starting index
        configs_processed = i - start_index + 1
        if configs_processed % FILE_SIZE == 0:
            file_name = output_path / f"co_{file_count}.pkl"
            with open(file_name, "wb") as f:
                pickle.dump(confs, f)
            logger.info(f"Saved {len(confs)} configurations to {file_name}")
            confs = []
            file_count += 1

    if len(confs) > 0:
        file_name = output_path / f"co_{file_count}.pkl"
        with open(file_name, "wb") as f:
            pickle.dump(confs, f)
        logger.info(f"Saved {len(confs)} configurations to {file_name}")

    logger.info(f"Processing complete. Last file created: co_{file_count}.pkl")


def main(index):
    dataset = dataset_info[index]
    dataset_path = dataset["path"]
    dataset_name = dataset["name"]
    output_path = Path(f"./{dataset_name}_pickles")
    pickle_omol(dataset_path, output_path, dataset_name)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.info("Usage: python omol_to_pickle.py <dataset_index>")
        logger.info("Dataset indices:")
        for i, ds in enumerate(dataset_info):
            logger.info(f"  {i}: {ds['name']}")
        sys.exit(1)

    try:
        dataset_index = int(sys.argv[1])
        if dataset_index < 0 or dataset_index >= len(dataset_info):
            raise ValueError(f"Index must be between 0 and {len(dataset_info) - 1}")
    except ValueError as e:
        logger.info(f"Error: {e}")
        sys.exit(1)

    main(dataset_index)

from itertools import islice
from pathlib import Path
from pickle import dump
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from ase.stress import voigt_6_to_full_3x3_stress
from fairchem.core.datasets import AseDBDataset
from tqdm import tqdm


def batched(configs, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    configs = iter(configs)
    while True:
        batch = list(islice(configs, n))
        if len(batch) == 0:
            break
        yield batch


def process_batch(batch, dataset, dbname, batch_ix):
    atoms = [dataset.get_atoms(x) for x in batch]
    id_atom = list(zip(batch, atoms))
    configs = []
    for id, config in id_atom:
        config.info["_name"] = f"sAlex__{config.info['sid']}__id_{id}"
        res = config.calc.results
        config.info["stress"] = voigt_6_to_full_3x3_stress(res["stress"])
        config.info["forces"] = res["forces"]
        config.info["energy"] = res["energy"]
        configs.append(config)
    assert len(configs) <= 10000
    output_dir = Path(f"salex_train_pickles/salex_{dbname}")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"salex_{dbname}_{batch_ix}.pkl"
    with output_path.open("wb") as f:
        dump(configs, f)


def salex_reader(fp):
    dataset = AseDBDataset(config=dict(src=[fp]))
    dbname = fp.stem
    id_batches = list(batched(dataset.ids, 10000))
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_batch, batch, dataset, dbname, batch_ix)
            for batch_ix, batch in enumerate(id_batches)
        ]
        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()  # To raise any exceptions that occurred


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pickles_from_salex.py <filepath>")
        sys.exit(1)

    filepath = Path(sys.argv[1])
    if not filepath.exists():
        print(f"File {filepath} does not exist.")
        sys.exit(1)
    salex_reader(filepath)

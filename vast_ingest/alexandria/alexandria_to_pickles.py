import json
import sys
from itertools import islice
from pathlib import Path
from pickle import dump
from time import time
import os

from colabfit.tools.configuration import AtomicConfiguration
from pymatgen.core import Structure

DATASET_NAME = "Alexandria_geometry_optimization_paths_PBE_3D"
TASK_ID = int(os.getenv("SLURM_ARRAY_TASK_ID"))


def batched(configs, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    configs = iter(configs)
    while True:
        batch = list(islice(configs, n))
        if len(batch) == 0:
            break
        yield batch


def process_batch(fp: Path):
    start = time()
    with fp.open("r") as f:
        data = json.load(f)
    print("len data", len(data))
    configs = []
    file_ix = 0
    output_dir = Path("alexandria_pickles/alexandria_3d")
    output_dir.mkdir(parents=True, exist_ok=True)
    for ix, id in enumerate(data):
        val = data[id]
        for j, trajectory in enumerate(val):
            # cs_name_match = f"alexandria_3d__file_{fp.stem}__id_{id}__trajectory_{j}"
            # cs_label_match = None
            # cs_name = f"{DATASET_NAME}__{fp.stem}__{id}__{j}"
            # cs_description = f"Alexandria 3D PBE trajectory number {j} for material {id} from file {fp.name}"  # noqa
            # CSS[cs_name] = {}
            # CSS[cs_name]["name"] = cs_name
            # CSS[cs_name]["description"] = cs_description
            # CSS[cs_name]["ids"] = []
            # (cs_name_match, cs_label_match, cs_name, cs_description)]
            input = {}
            kpoints = trajectory.get("kpoints")
            enaug = trajectory.get("ENAUG")
            enmax = trajectory.get("ENMAX")
            prec = trajectory.get("PREC")
            if kpoints:
                input["kpoints"] = kpoints
            if enaug:
                input["ENAUG"] = enaug
            if enmax:
                input["ENMAX"] = enmax
            if prec:
                input["PREC"] = prec
            for i, step in enumerate(trajectory["steps"]):
                struct = Structure.from_dict(step["structure"])
                config = struct.to_ase_atoms()
                labels = [
                    f"alexandria_id:{id}",
                ]
                config.info["_name"] = (
                    f"alexandria_3d__file_{fp.stem}__id_{id}__trajectory_{j}__frame_{i}"
                )
                config.info["_labels"] = labels
                config.info["energy"] = step["energy"]
                config.info["forces"] = step["forces"]
                config.info["stress"] = step["stress"]
                config.info["input"] = input
                aconfig = AtomicConfiguration.from_ase(config)
                configs.append(aconfig)
                if len(configs) == 10000:
                    output_path = output_dir / f"alexandria_3d_{fp.stem}_{file_ix}.pkl"
                    with output_path.open("wb") as f:
                        dump(configs, f)
                    file_ix += 1
                    configs = []
    if len(configs) > 0:
        output_path = output_dir / f"alexandria_3d_{fp.stem}_{file_ix}.pkl"
        with output_path.open("wb") as f:
            dump(configs, f)
    print("completed")
    print(f"Processed {fp} in {time() - start:.2f} seconds")


def alex_reader(fp):
    files = sorted(list(fp.rglob("*.json")))
    file = files[TASK_ID]
    print("processing", file)
    process_batch(file)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pickles_from_alexandria.py <filepath>")
        sys.exit(1)

    filepath = Path(sys.argv[1])
    if not filepath.exists():
        print(f"File {filepath} does not exist.")
        sys.exit(1)
    alex_reader(filepath)

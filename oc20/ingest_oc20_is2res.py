from argparse import ArgumentParser
from ase.io import read
from colabfit import ATOMS_LABELS_FIELD, ATOMS_NAME_FIELD
from colabfit.tools.converters import AtomicConfiguration
from colabfit.tools.database import MongoDatabase
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
    free_energy_pd,
)
from pathlib import Path
import sys
from tqdm import tqdm

BATCH_SIZE = 10

AUTHORS = [
    "Lowik Chanussot",
    "Abhishek Das",
    "Siddharth Goyal",
    "Thibaut Lavril",
    "Muhammed Shuaibi",
    "Morgane Riviere",
    "Kevin Tran",
    "Javier Heras-Domingo",
    "Caleb Ho",
    "Weihua Hu",
    "Aini Palizhati",
    "Anuroop Sriram",
    "Brandon Wood",
    "Junwoong Yoon",
    "Devi Parikh",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
]
LINKS = [
    "https://arxiv.org/abs/2010.09990",
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md",
]
DS_DESC = "All configurations from the OC20 IS2RES training set"
DATASET = "OC20-IS2RES"
DATASET_FP = Path("is2res_train_trajectories")
GLOB_STR = "*.extxyz"


def reader(filepath, ref_e_dict):
    fp_stem = filepath.stem
    configs = []
    ase_configs = read(filepath, index=":")

    for i, ase_config in enumerate(ase_configs):
        config = AtomicConfiguration(
            positions=ase_config.positions,
            numbers=ase_config.numbers,
            pbc=ase_config.pbc,
            cell=ase_config.cell,
        )
        config.info = ase_config.info
        config.info["forces"] = ase_config.arrays["forces"]
        config.info["ref_energy"] = ref_e_dict[fp_stem]
        config.info["oc_id"] = fp_stem
        config.info["name"] = f"{filepath.parts[-3]}_{fp_stem}_{i}"

        configs.append(config)

    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "reference-energy": {"field": "ref_energy", "units": "eV"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "reference_energy": {"field": "ref_energy"},
                },
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "free_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "reference-energy": {"field": "ref_energy", "units": "eV"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }
    co_md_map = {
        "bulk_id": {"field": "bulk_id"},
        "ads_id": {"field": "ads_id"},
        "bulk_symbols": {"field": "bulk_symbols"},
        "ads_symbols": {"field": "ads_symbols"},
        "miller_index": {"field": "miller_index"},
        "shift": {"field": "shift"},
        "adsorption_site": {"field": "adsorption_site"},
        "oc_class": {"field": "class"},
        "oc_anomaly": {"field": "anomaly"},
        "frame": {"field": "frame"},
        "oc-id": {"field": "oc_id"},
    }

    # bulk_id = f.split("/")[-1]
    with open(DATASET_FP / "system.txt", "r") as f:
        ref_text = [x.strip().split(",") for x in f.readlines()]
        ref_e_dict = {k: float(v) for k, v in ref_text}
    name_field = "name"
    labels_field = "labels"
    ai = 0
    ids = []
    fps = list(DATASET_FP.rglob(GLOB_STR))
    n_batches = len(fps) // BATCH_SIZE
    leftover = len(fps) % BATCH_SIZE
    indices = [((b * BATCH_SIZE, (b + 1) * BATCH_SIZE)) for b in range(n_batches)]
    if leftover:
        indices.append((BATCH_SIZE * n_batches, len(fps)))
    for batch in tqdm(indices):
        configurations = []
        beg, end = batch
        for fi, fpath in enumerate(fps[beg:end]):
            new = reader(fpath, ref_e_dict)

            for atoms in new:
                if name_field in atoms.info:
                    name = []
                    name.append(atoms.info[name_field])
                    atoms.info[ATOMS_NAME_FIELD] = name
                else:
                    raise RuntimeError(
                        f"Field {name_field} not in atoms.info for index "
                        f"{ai}. Set `name_field=None` "
                        "to use `default_name`."
                    )

                if labels_field not in atoms.info:
                    atoms.info[ATOMS_LABELS_FIELD] = set()
                else:
                    atoms.info[ATOMS_LABELS_FIELD] = set(atoms.info[labels_field])
                ai += 1
                configurations.append(atoms)

        ids.extend(
            list(
                client.insert_data(
                    configurations,
                    co_md_map=co_md_map,
                    property_map=property_map,
                    generator=False,
                    verbose=False,
                )
            )
        )

        all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    args = sys.argv[1:]
    main(args)

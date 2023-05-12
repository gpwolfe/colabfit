"""
author:gpwolfe

Data can be downloaded from:

Download link:

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from ase.io import read
from colabfit import ATOMS_NAME_FIELD, ATOMS_LABELS_FIELD
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import pickle
import sys
from tqdm import tqdm

BATCH_SIZE = 100

DATASET_FP = Path("oc22_trajectories")
DATASET = "OC22"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"
LINKS = [
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md#"
    "open-catalyst-2022-oc22",
    "https://opencatalystproject.org/",
    "https://doi.org/10.48550/arXiv.2206.08917",
]
AUTHORS = [
    "Richard Tran",
    "Janice Lan",
    "Muhammed Shuaibi",
    "Brandon M. Wood",
    "Siddharth Goyal",
    "Abhishek Das",
    "Javier Heras-Domingo",
    "Adeesh Kolluru",
    "Ammar Rizvi",
    "Nima Shoghi",
    "Anuroop Sriram",
    "Felix Therrien",
    "Jehad Abed",
    "Oleksandr Voznyy",
    "Edward H. Sargent",
    "Zachary Ulissi",
    "C. Lawrence Zitnick",
]
DS_DESC = (
    "A database of training trajectories for predicting catalytic"
    "reactions on oxide surfaces. OC22 is meant to complement OC20, which did not"
    "contain oxide surfaces."
)
ELEMENTS = {
    "Ag",
    "Al",
    "As",
    "Au",
    "Ba",
    "Be",
    "Bi",
    "C",
    "Ca",
    "Cd",
    "Ce",
    "Co",
    "Cr",
    "Cs",
    "Cu",
    "Fe",
    "Ga",
    "Ge",
    "H",
    "Hf",
    "Hg",
    "In",
    "Ir",
    "K",
    "Li",
    "Lu",
    "Mg",
    "Mn",
    "Mo",
    "N",
    "Na",
    "Nb",
    "Ni",
    "O",
    "Os",
    "Pb",
    "Pd",
    "Pt",
    "Rb",
    "Re",
    "Rh",
    "Ru",
    "Sb",
    "Sc",
    "Se",
    "Si",
    "Sn",
    "Sr",
    "Ta",
    "Te",
    "Ti",
    "Tl",
    "V",
    "W",
    "Y",
    "Zn",
    "Zr",
}
GLOB_STR = "*.traj"

train_val = dict()
with open("oc22_trajectories/trajectories/oc22/train_is2re_t.txt", "r") as f:
    keys = set(f.readlines())
    for key in keys:
        train_val[key.strip()] = "train_is2re"
with open("oc22_trajectories/trajectories/oc22/train_s2ef_t.txt", "r") as f:
    keys = set(f.readlines())
    for key in keys:
        train_val[key.strip()] = "train_s2ef"
with open("oc22_trajectories/trajectories/oc22/val_id_is2re_t.txt", "r") as f:
    keys = set(f.readlines())
    for key in keys:
        train_val[key.strip()] = "val_id_is2re"
with open("oc22_trajectories/trajectories/oc22/val_id_s2ef_t.txt", "r") as f:
    keys = set(f.readlines())
    for key in keys:
        train_val[key.strip()] = "val_id_s2ef"
with open("oc22_trajectories/trajectories/oc22/val_ood_is2re_t.txt", "r") as f:
    keys = set(f.readlines())
    for key in keys:
        train_val[key.strip()] = "val_ood_is2re"
with open("oc22_trajectories/trajectories/oc22/val_ood_s2ef_t.txt", "r") as f:
    keys = set(f.readlines())
    for key in keys:
        train_val[key.strip()] = "val_ood_s2ef"

OC_PICKLE = Path("oc22_metadata.pkl")
with open(OC_PICKLE, "rb") as f:
    oc_meta = pickle.load(f)

CONFIG_META = dict()
for sid, vals in oc_meta.items():
    if vals.get("traj_id"):
        CONFIG_META[vals["traj_id"]] = {
            "lmdb-system-id": sid,
            "miller-index": vals["miller_index"],
            "bulk-symbols": vals["bulk_symbols"],
            "slab-sid": vals.get("slab_sid"),
            "nads": vals["nads"],
            "ads-symbols": vals.get("ads_symbols"),
        }


def reader(filepath):
    traj_id = filepath.stem
    key = filepath.name
    configs = []
    ase_configs = read(filepath, index=":")
    for i, ase_config in enumerate(ase_configs):
        config = AtomicConfiguration(
            positions=ase_config.positions,
            numbers=ase_config.numbers,
            pbc=ase_config.pbc,
            cell=ase_config.cell,
        )
        config.info["energy"] = ase_config.get_potential_energy()
        config.info["forces"] = ase_config.get_forces()
        config.info["name"] = f"{train_val[key]}__{traj_id}__{i}"
        config.info["traj_id"] = traj_id
        config.info["config_meta"] = CONFIG_META[traj_id]
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
    }

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }
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
            new = reader(fpath)

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
                    co_md_map={"lmdb_metadata": {"field": "config_meta"}},
                    property_map=property_map,
                    generator=False,
                    verbose=False,
                )
            )
        )

    all_co_ids, all_do_ids = list(zip(*ids))
    descriptions = {
        "train_is2re": "Training configurations for initial structure to relaxed total"
        "energy task",
        "train_s2ef": "Training configurations for structure to total energy and forces"
        "task",
        "val_id_is2re": "Validation configurations for initial structure to relaxed "
        "total energy task",
        "val_id_s2ef": "Validation configurations for structure to total energy and "
        "forces task",
        "val_ood_is2re": "Unseen test configurations for initial structure to relaxed "
        "total energy task",
        "val_ood_s2ef": "Unseen test configurations for structure to total energy and "
        "forces task",
    }
    cs_regexes = []
    for key, val in descriptions.items():
        cs_regexes.append([f"{DATASET}_{key}", f"{key}.*", val])

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

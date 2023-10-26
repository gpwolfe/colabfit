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
from colabfit.tools.database import MongoDatabase, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

from pathlib import Path
import pickle
import sys
from tqdm import tqdm

BATCH_SIZE = 100

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts_large/oc_22/")  # HSRN
# DATASET_FP = Path("/scratch/work/martiniani/for_gregory/oc22/oc22")  # Greene
# DATASET_FP = Path("data/oc22/")  # remove
TXT_FP = DATASET_FP / "oc22_trajectories/trajectories/oc22/"
DATASET = "OC22"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1021/acscatal.2c05426"
DATA_LINK = (
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md#"
    "open-catalyst-2022-oc22"
)
LINKS = [
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md#"
    "open-catalyst-2022-oc22",
    "https://opencatalystproject.org/",
    "https://doi.org/10.1021/acscatal.2c05426",
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
    "Open Catalyst 2022 (OC22) is a database of training trajectories for predicting "
    "catalytic reactions on oxide surfaces meant to complement OC20, which did not "
    "contain oxide surfaces."
)
ELEMENTS = None


OC_PICKLE = Path(DATASET_FP / "oc22_metadata.pkl")
with open(OC_PICKLE, "rb") as f:
    oc_meta = pickle.load(f)

CONFIG_META = dict()
for sid, vals in oc_meta.items():
    traj_id = vals.get("traj_id")
    if traj_id is not None:
        CONFIG_META[traj_id] = {
            "lmdb-system-id": sid,
            "miller-index": vals["miller_index"],
            "bulk-symbols": vals["bulk_symbols"],
            "slab-sid": vals.get("slab_sid"),
            "nads": vals["nads"],
            "ads-symbols": vals.get("ads_symbols"),
        }
co_md = {
    "lmdb-system-id": {"field": "lmdb-system-id"},
    "miller-index": {"field": "miller_index"},
    "bulk-symbols": {"field": "bulk_symbols"},
    "slab-sid": {"field": "slab_sid"},
    "num-adsorbates": {"field": "nads"},
    "adsorbate-symbols": {"field": "ads_symbols"},
    "traj-id": {"field": "traj_id"},
}

ds_name_path_desc = (
    (
        "OC22-IS2RE-Train",
        "is2re_train",
        "train_is2re_t.txt",
        "Training configurations for the initial structure to relaxed total energy "
        "(IS2RE) task of OC22. ",
    ),
    (
        "OC22-S2EF-Train",
        "s2ef_train",
        "train_s2ef_t.txt",
        "Training configurations for the structure to total energy and forces task "
        "(S2EF) of OC22. ",
    ),
    (
        "OC22-IS2RE-Validation-in-domain",
        "is2re_val",
        "val_id_is2re_t.txt",
        "In-domain validation configurations for the initial structure to relaxed "
        "total energy (IS2RE) task of OC22. ",
    ),
    (
        "OC22-S2EF-Validation-in-domain",
        "s2ef_val_id",
        "val_id_s2ef_t.txt",
        "In-domain validation configurations for the structure to total energy and "
        "forces (S2EF) task of OC22. ",
    ),
    (
        "OC22-IS2RE-Validation-out-of-domain",
        "is2re_val_ood",
        "val_ood_is2re_t.txt",
        "Out-of-domain validation configurations for the initial structure to "
        "relaxed total energy (IS2RE) task of OC22. ",
    ),
    (
        "OC22-S2EF-Validation-out-of-domain",
        "s2ef_val_ood",
        "val_ood_s2ef_t.txt",
        "Out-of-domain validation configurations for the structure to total energy "
        "and forces (S2EF) task of OC22. ",
    ),
)


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


def reader(filepath):
    traj_id = filepath.stem
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
        config.info["traj_id"] = traj_id
        if CONFIG_META.get(traj_id) is not None:
            config.info.update(CONFIG_META[traj_id])
        configs.append(config)
    return configs


def main(argv, dataset):
    ds_name, co_name, path, desc = dataset
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
    ds_id = generate_ds_id()

    with open(TXT_FP / path, "r") as f:
        keys = set([x.strip() for x in f.readlines()])
    fps = [next(DATASET_FP.rglob(key)) for key in keys]
    labels_field = "labels"
    ai = 0
    ids = []
    n_batches = len(fps) // BATCH_SIZE
    # n_batches = 2  # for local testing
    leftover = len(fps) % BATCH_SIZE
    indices = [((b * BATCH_SIZE, (b + 1) * BATCH_SIZE)) for b in range(n_batches)]
    if leftover:
        indices.append((BATCH_SIZE * n_batches, len(fps)))
    for batch in tqdm(indices):
        configurations = []
        beg, end = batch
        for fi, fpath in enumerate(fps[beg:end]):
            new = reader(fpath)

            for i, atoms in enumerate(new):
                name = []
                name.append(f"{co_name}_{i}")
                atoms.info[ATOMS_NAME_FIELD] = name

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
                    ds_id=ds_id,
                    co_md_map=co_md,
                    property_map=property_map,
                    generator=False,
                    verbose=False,
                )
            )
        )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        # cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=ds_name,
        authors=AUTHORS,
        links=LINKS,
        description=f"{desc}{DS_DESC}",
        verbose=True,
    )


if __name__ == "__main__":
    for dataset in ds_name_path_desc:
        main(sys.argv[1:], dataset)

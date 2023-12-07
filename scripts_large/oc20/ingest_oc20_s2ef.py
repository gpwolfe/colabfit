from argparse import ArgumentParser
from ase.io import read
from colabfit import ATOMS_LABELS_FIELD, ATOMS_NAME_FIELD
from colabfit.tools.converters import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
    free_energy_pd,
)
import itertools
import multiprocessing
from pathlib import Path
import sys
from tqdm import tqdm

BATCH_SIZE = 512

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
PUBLICATION = "https://doi.org/10.1021/acscatal.0c04525"
DATA_LINK = "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md"
LINKS = [
    "https://doi.org/10.1021/acscatal.0c04525",
    "https://github.com/Open-Catalyst-Project/ocp/blob/main/DATASET.md",
]
# DATASETS = {
#     "OC20TEST": {
#         "fp": Path("s2ef_train_200K/uc"),
#         "desc": "Testing dataset of 200K, REMOVE FOR INGEST",
#     },
#     "OC20TEST2": {
#         "fp": Path("s2ef_val_id/uc"),
#         "desc": "Testing dataset of 200K, REMOVE FOR INGEST",
#     },
# }
DATASETS = {
    "OC20-S2EF-MD": {
        "fp": Path("s2ef_md/uc"),
        "desc": "A subset of training configurations from the Open Catalyst 2020 "
        "(OC20) Structure to Energy and Forces (S2EF) task for which short, "
        "high-temperature ab initio MD trajectories were gathered. Corresponds to the "
        '"MD data" split from the OC20 GitHub site',
    },
    "OC20-S2EF-Rattled": {
        "fp": Path("s2ef_rattled/uc"),
        "desc": "A subset of training configurations from the Open Catalyst 2020 "
        "(OC20) Structure to Energy and Forces (S2EF) task in which the atomic "
        'positions have been randomly perturbed, corresponding to the "Rattled data" '
        "split from the OC20 GitHub site.",
    },
    "OC20-S2EF-Train-All": {
        "fp": Path("s2ef_train_all/uc"),
        "desc": "All configurations from the Open Catalyst 2020 (OC20) Structure to "
        'Energy and Forces (S2EF) training set, corresponding to the "Train/all" split '
        "from the OC20 GitHub site.",
    },
}

GLOB_STR = "*.extxyz"
NAME_FIELD = "name"
LABELS_FIELD = "labels"

# def tform(c):
#     print(c.info["miller_index"])
#     print(c.info["adsorption_site"])
#     c.info["miller_index"] = c.info["miller_index"].remove("_JSON ")
#     c.info["adsorption_site"] = c.info["adsorption_site"].remove("_JSON ")

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "ref_energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "reference_energy": {"field": "ref_energy"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
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


def reader(filepath):
    fp_stem = filepath.stem
    txt_file = fp_stem + ".txt"
    configs = []
    ase_configs = read(filepath, index=":")
    with open(filepath.parent / txt_file, "r") as f:
        text = f.readlines()

    for i, ase_config in enumerate(ase_configs):
        oc_id, frame, ref_energy = text[i].strip().split(",")
        config = AtomicConfiguration().from_ase(ase_config)
        config.info["ref_energy"] = float(ref_energy)
        config.info["frame"] = frame
        config.info["oc_id"] = oc_id
        config.info["name"] = f"{filepath.parts[-3]}_{fp_stem}_{frame}"

        configs.append(config)

    return configs


def read_for_pool(filepath):
    configurations = []
    new = reader(filepath)
    for i, atoms in enumerate(new):
        if NAME_FIELD in atoms.info:
            atoms.info[ATOMS_NAME_FIELD] = [atoms.info[NAME_FIELD]]
        else:
            raise RuntimeError(
                f"Field {NAME_FIELD} not in atoms.info for index "
                f"{i} in {filepath}. Set `name_field=None` "
                "to use `default_name`."
            )

        if LABELS_FIELD not in atoms.info:
            atoms.info[ATOMS_LABELS_FIELD] = set()
        else:
            atoms.info[ATOMS_LABELS_FIELD] = set(atoms.info[LABELS_FIELD])
        configurations.append(atoms)

    return configurations


def get_configs(filepaths, client, co_md_map, property_map, ds_id, nprocs):
    pool = multiprocessing.Pool(nprocs)

    configurations = list(
        itertools.chain.from_iterable(pool.map(read_for_pool, filepaths))
    )

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )
    return ids


def upload_configs(ds_vals, client, co_md_map, property_map, ds_id, nprocs):
    ids = []
    fps = list(ds_vals["fp"].rglob(GLOB_STR))
    n_batches = len(fps) // BATCH_SIZE
    leftover = len(fps) % BATCH_SIZE
    indices = [((b * BATCH_SIZE, (b + 1) * BATCH_SIZE)) for b in range(n_batches)]
    if leftover:
        indices.append((BATCH_SIZE * n_batches, len(fps)))
    for batch in tqdm(indices):
        beg, end = batch
        filepaths = fps[beg:end]
        ids.extend(
            get_configs(
                filepaths,
                client=client,
                co_md_map=co_md_map,
                property_map=property_map,
                ds_id=ds_id,
                nprocs=nprocs,
            )
        )

    all_co_ids, all_do_ids = list(zip(*ids))

    return all_do_ids


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
    nprocs = args.nprocs
    client = MongoDatabase(
        args.db_name, nprocs=nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    # bulk_id = f.split("/")[-1]

    for dataset, ds_vals in DATASETS.items():
        ds_id = generate_ds_id()
        do_hashes = upload_configs(
            ds_vals,
            client=client,
            co_md_map=co_md_map,
            property_map=property_map,
            ds_id=ds_id,
            nprocs=nprocs,
        )
        client.insert_dataset(
            do_hashes=do_hashes,
            ds_id=ds_id,
            name=dataset,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=ds_vals["desc"],
            verbose=True,
        )


if __name__ == "__main__":
    args = sys.argv[1:]
    main(args)

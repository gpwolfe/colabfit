"""
author: Gregory Wolfe

Properties
----------
energy
virial
forces

Other properties added to metadata
----------------------------------

File notes
----------

xyz file header
lattice
energy
virial
Properties=species:S:1:pos:R:3:force:R:3

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import iread

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/unep")
DATASET_NAME = "UNEP-v1_2023"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.48550/arXiv.2311.04732"
DATA_LINK = "https://zenodo.org/doi/10.5281/zenodo.10081676"
LINKS = [
    "https://zenodo.org/doi/10.5281/zenodo.10081676",
    "https://doi.org/10.48550/arXiv.2311.04732",
]
AUTHORS = [
    "Keke Song",
    "Rui Zhao",
    "Jiahui Liu",
    "Yanzhou Wang",
    "Eric Lindgren",
    "Yong Wang",
    "Shunda Chen",
    "Ke Xu",
    "Ting Liang",
    "Penghua Ying",
    "Nan Xu",
    "Zhiqiang Zhao",
    "Jiuyang Shi",
    "Junjie Wang",
    "Shuang Lyu",
    "Zezhu Zeng",
    "Shirong Liang",
    "Haikuan Dong",
    "Ligang Sun",
    "Yue Chen",
    "Zhuhua Zhang",
    "Wanlin Guo",
    "Ping Qian",
    "Jian Sun",
    "Paul Erhart",
    "Tapio Ala-Nissila",
    "Yanjing Su",
    "Zheyong Fan",
]
DATASET_DESC = (
    "This dataset contains training data for UNEP-v1 (version 1 of Unified "
    "NeuroEvolution Potential), a model implemented in GPUMD. Included are 16 separate "
    "configuration sets for individual elemental metals, as well as their 120 unique "
    "pairs."
)
ELEMENTS = None
GLOB_STR = "*.xyz"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "energy-cutoff": {"value": "600eV"},
    "k-point": {"value": "gamma-centered 0.2/Ang"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "force", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "virial", "units": "kbar"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

EL_COMBOS = [fp.stem for fp in DATASET_FP.rglob("*.xyz")]
CSS = [
    (
        f"{DATASET_NAME}_{el_combo}",
        {"names": {"$regex": f"_{el_combo}_"}},
        f"Configurations of {el_combo} from {DATASET_NAME} dataset",
    )
    for el_combo in EL_COMBOS
]


def reader(fp):
    name = fp.stem
    for i, c in enumerate(iread(fp, index=":", format="extxyz")):
        c.info["name"] = f"unep_v1_{name}_{i}"
        yield c


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_ids = []
    for i, (name, query, desc) in enumerate(CSS):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=query,
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author:gpwolfe

Data can be downloaded from:
https://www.aissquare.com/datasets/detail?pageType=datasets&name=deepmd-se-dataset

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
energy
forces
virial

Other properties added to metadata
----------------------------------

File notes
----------
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/deepmd-se")
DATASET = "DeePMD_SE"
PUBLICATION = "https://doi.org/10.48550/arXiv.1805.09003"
DATA_LINK = (
    "https://www.aissquare.com/datasets/detail?pageType=datasets&name=deepmd-se-dataset"
)
LINKS = [
    "https://www.aissquare.com/datasets/detail?pageType=datasets&name=deepmd-se-"
    "dataset",
    "https://doi.org/10.48550/arXiv.1805.09003",
]
AUTHORS = [
    "Linfeng Zhang",
    "Jiequn Han",
    "Han Wang",
    "Wissam A. Saidi",
    "Roberto Car",
    "Weinan E",
]
DS_DESC = "127,000 configurations from a dataset used to benchmark and train\
 a modified DeePMD model called DeepPot-SE, or Deep Potential - Smooth Edition"
ELEMENTS = [
    "Co",
    "Cr",
    "Fe",
    "Mn",
    "Ni",
    "Ge",
    "Si",
    "Ti",
    "O",
    "Pt",
    "Mo",
    "S",
    "Al",
    "Cu",
    "N",
    "C",
    "H",
]
GLOB_STR = "box.npy"


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    a_types = []
    elem_map = dict()
    with open(filepath.parents[1] / "type_map.raw", "r") as f:
        types = [x.strip() for x in f.readlines()]
        for i, t in enumerate(types):
            elem_map[i] = t
    with open(filepath.parents[1] / "type.raw", "r") as f:
        a_types = f.readlines()
        if len(a_types) == 1:
            a_types = [int(float(x.strip())) for x in a_types[0].split()]
        else:
            a_types = [int(float(x.strip())) for x in a_types]
    symbols = list(map(lambda x: elem_map.get(x), a_types))
    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = props["force"].shape[1] // 3
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    virial = props.get("virial")
    if virial is not None:
        props["virial"] = virial.reshape(num_configs, 3, 3)
    props["symbols"] = symbols
    return props


def meta_namer(filepath):
    parts = filepath.parts
    if parts[-3] in ["Al2O3", "Cu", "Ge", "Si"]:
        return f"{parts[-3]}__{parts[-2]}", "NVT", "CP2K"
    elif parts[-4] in ["MoS2_Pt", "TiO2"]:
        return f"{parts[-4]}__{parts[-3]}_{parts[-2]}", "NVT", "CP2K"
    elif parts[-4] == "pyridine":
        return (
            f"{parts[-4]}__{parts[-3]}_{parts[-2]}",
            "DFT-PBE",
            "Quantum ESPRESSO",
        )
    elif parts[-5] == "HEA":
        return f"HEA_{parts[-4]}__{parts[-3]}_{parts[-2]}", "NVT", "CP2K"


def reader(filepath):
    name, methods, software = meta_namer(filepath)
    props = assemble_props(filepath)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["forces"][i]
        virial = props.get("virial")
        if virial is not None:
            c.info["virial"] = virial[i]
        # if energy is not None:
        c.info["energy"] = float(energy[i])
        c.info["methods"] = methods
        c.info["software"] = software

        c.info["name"] = f"{name}_{i}"
    return configs


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"field": "software"},
        "method": {"field": "methods"},
        # "": {"field": ""}
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
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_regexes = [
        [
            f"{DATASET}-Al2O3",
            "Al2O3__*",
            f"Al2O3 configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-Cu",
            "Cu__*",
            f"Cu configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-Ge",
            "Ge__*",
            f"Ge configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-HEA",
            "HEA__*",
            "CoCrFeMnNi high entropy alloy (HEA) configurations from "
            f"{DATASET} dataset",
        ],
        [
            f"{DATASET}-MoS2-Pt",
            "MoS2_Pt*",
            f"MoS2 + Pt configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-pyridine",
            "pyridine__*",
            f"Pyridine configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-Si",
            "Si__*",
            f"Si configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-TiO2",
            "TiO2__*",
            f"TiO2 configurations from {DATASET} dataset",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={
                "hash": {"$in": all_co_ids},
                "names": {"$regex": regex},
            },
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )
        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, ds_id=ds_id, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

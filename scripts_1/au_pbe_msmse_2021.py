"""
author:gpwolfe

Data can be downloaded from:
https://aisquare-zjk.oss-cn-zhangjiakou.aliyuncs.com/AIS-Square/datasets/Ag-PBE/Ag-PBE.zip

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

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/au_pbe_msmse_2021"
)
DATASET_FP = Path().cwd().parent / "data/au_pbe_msmse_2021"
DATASET = "Au-PBE_MSMSE_2021"
LICENSE = "https://www.gnu.org/licenses/lgpl-3.0-standalone.html"


SOFTWARE = "VASP, DP-GEN"
METHODS = "DFT-PBE-D3"
PUBLICATION = "https://doi.org/10.48550/arXiv.2108.06232"
DATA_LINK = "https://www.aissquare.com/datasets/detail?pageType=datasets&name=Au-PBE"
LINKS = [
    "https://www.aissquare.com/datasets/detail?pageType=datasets&name=Au-PBE",
    "https://doi.org/10.48550/arXiv.2108.06232",
]
AUTHORS = [
    "Yinan Wang",
    "Xiaoyang Wang",
    "Linfeng Zhang",
    "Ben Xu",
    "Han Wang",
]
DS_DESC = "Approximately 20,000 configurations of Au used as part of a \
 training dataset for a DP-GEN-based ML model for a Ag-Au nanoalloy \
 potential."
ELEMENTS = ["Au"]
GLOB_STR = "box.npy"

ELEM_MAP = {0: "Au"}


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    a_types = []
    with open(filepath.parents[1] / "type.raw", "r") as f:
        a_types = f.readlines()
        if len(a_types) == 1:
            a_types = [int(x.strip()) for x in a_types[0].split()]
        else:
            a_types = [int(x.strip()) for x in a_types]
    symbols = list(map(lambda x: ELEM_MAP.get(x), a_types))
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


def reader(filepath):
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

        c.info["name"] = f"{filepath.parts[-3]}_{filepath.parts[-2]}_{i}"
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
    ds_id = generate_ds_id()

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {"encut": {"value": 650, "units": "eV"}},
        },
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
                "forces": {"field": "forces", "units": "eV/angstrom"},
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
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        data_license=LICENSE,
        description=DS_DESC,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

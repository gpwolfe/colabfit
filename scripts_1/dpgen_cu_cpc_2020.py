"""
author:gpwolfe

Data can be downloaded from:
https://www.aissquare.com/datasets/detail?pageType=datasets&name=Cu-dpgen

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
The last author name is Weinan E -- this is not a misspelling

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
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/dpgen_cu_cpc_2020"
)
DATASET_FP = Path().cwd().parent / "data/dpgen_cu_cpc_2020"
DATASET = "DP-GEN_Cu"
LICENSE = "https://www.gnu.org/licenses/lgpl-3.0-standalone.html"


SOFTWARE = "VASP"
METHODS = "DFT-PBE"
PUBLICATION = "https://doi.org/10.1016/j.cpc.2020.107206"
DATA_LINK = "https://www.aissquare.com/datasets/detail?pageType=datasets&name=Cu-dpgen"
LINKS = [
    "https://www.aissquare.com/datasets/detail?pageType=datasets&name=Cu-dpgen",
    "https://doi.org/10.1016/j.cpc.2020.107206",
]
AUTHORS = [
    "Yuzhi Zhang",
    "Haidi Wang",
    "Weijie Chen",
    "Jinzhe Zeng",
    "Linfeng Zhang",
    "Han Wang",
    "Weinan E",
]
DS_DESC = (
    "Approximately 15,000 configurations of copper used to demonstrate "
    "the DP-GEN data generator for PES machine learning models."
)
ELEMENTS = ["Cu"]
GLOB_STR = "box.npy"

ELEM_MAP = {0: "Cu"}


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

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
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
    ds_id = generate_ds_id()
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
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=False,
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

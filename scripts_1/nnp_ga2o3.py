"""
author:gpwolfe

Data can be downloaded from:

Download link:


Extract to project folder
tar -xf example.tar -C  <project_dir>/data/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

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
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/nnp_ga2o3/NNP_Ga2O3-master"
)
DATASET_FP = Path().cwd().parent / "data/nnp_ga2o3"
DATASET = "NNP-Ga2O3"

SOFTWARE = "CP2K"
METHODS = "DFT-QUICKSTEP"

DATA_LINK = "https://github.com/RuiyangLi6/NNP_Ga2O3"
PUBLICATION = "https://doi.org/10.1063/5.0025051"
LINKS = [
    "https://github.com/RuiyangLi6/NNP_Ga2O3",
    "https://doi.org/10.1063/5.0025051",
]
AUTHORS = [
    "Ruiyang Li",
    "Zeyu Liu",
    "Andrew Rohskopf",
    "Kiarash Gordiz",
    "Asegun Henry",
    "Eungkyu Lee",
    "Tengfei Luo",
]
DS_DESC = (
    "9,200 configurations of beta-Ga2O3, including two configuration "
    "sets. One contains DFT data for 8400 configurations simulated between "
    "temperatures of 50K - 600K. The second contains configurations with 0K "
    "simulation temperature."
)
ELEMENTS = ["Ga", "O"]
GLOB_STR = "energy.npy"

ELEM_KEY = {0: "Ga", 1: "O"}


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    type_path = list(filepath.parents[1].glob("type.raw"))[0]

    with open(type_path, "r") as f:
        nums = f.read().rstrip().split(" ")
        props["symbols"] = [ELEM_KEY[int(num)] for num in nums]

    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = len(props["symbols"])
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
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
        c.info["energy"] = float(energy[i])
        c.info["name"] = f"NNP-Ga2O3_{filepath.parts[-3]}_{filepath.parts[-2]}_{i}"
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

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "basis-set": {"value": "TZVP"},
        "input": {
            "pseudopotentials": "GTH",
            "encut": {"value": 800, "units": "rydberg"},
            "k-points": "gamma-point",
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
    cs_regexes = [
        [
            f"{DATASET}_with_0K",
            ".*with0K.*",
            f"All configurations from {DATASET} dataset, including those simulated "
            "between temperatures 0K - 600K",
        ],
        [
            f"{DATASET}_without_0K",
            ".*without0K.*",
            f"Configurations from {DATASET} dataset simulated between temperatures "
            "50K - 600K",
        ],
    ]
    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author:gpwolfe

Data can be downloaded from:

Download link:

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
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/ti_npjcm_2021"
)
# comment out, local testing
DATASET_FP = Path().cwd().parent / ("data/ti_npjcm_2021")
DATASET = "Ti_NPJCM_2021"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

DATA_LINK = "https://www.aissquare.com/datasets/detail?pageType=datasets&name=Ti"
PUBLICATION = "https://doi.org/10.1038/s41524-021-00661-y"
LINKS = [
    "https://www.aissquare.com/datasets/detail?pageType=datasets&name=Ti",
    "https://doi.org/10.1038/s41524-021-00661-y",
]
AUTHORS = [
    "Tongqi Wen",
    "Rui Wang",
    "Lingyu Zhu",
    "Linfeng Zhang",
    "Han Wang",
    "David J. Srolovitz",
    "Zhaoxuan Wu",
]
DS_DESC = (
    "Approximately 7,400 configurations of titanium used for training a deep "
    "potential using the DeePMD-kit molecular dynamics package and DP-GEN training "
    "scheme."
)
ELEMENTS = ["Ti"]
GLOB_STR = "box.npy"


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))

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
    props["symbols"] = ["Ti" for x in range(num_atoms)]
    return props


# Above used with below:
def namer(filepath):
    if "gamma_line" in filepath.parts[-8:]:
        return "specialization"
    elif "dpgen" in filepath.parts[-6]:
        return "dp_gen"
    elif "init" in filepath.parts[-8] or "init" in filepath.parts[-9]:
        return "initialization"
    elif "init" in filepath.parts[-7]:
        return "initialization"
    else:
        print(filepath)
        return "uncaught"


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

        c.info["name"] = namer(filepath)
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
            f"{DATASET}_initialization",
            "initialization",
            f"Configurations from initialization step of DP model for "
            f"{DATASET} dataset",
        ],
        [
            f"{DATASET}_DP_GEN",
            "dp_gen",
            f"Configurations from DeePMD DPGEN training step for {DATASET} " f"dataset",
        ],
        [
            f"{DATASET}_specialization",
            "specialization",
            f"Configurations from specialization step for {DATASET} dataset, "
            f"where application-specific structures are created, such as "
            f"configurations sheared along the gamma-line.",
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
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

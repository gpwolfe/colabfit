"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.24435/materialscloud:2020.0037/v1

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
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import re
import sys

DATASET_FP = Path().cwd()
DATASET = "HO-LiMoNiTi-NPJCM-2020"

SOFTWARE = "VASP"
LINKS = [
    "https://doi.org/10.24435/materialscloud:2020.0037/v1",
    "https://doi.org/10.1038/s41524-020-0323-8",
]
AUTHORS = "A. Cooper, J. Kästner1, A. Urban, N. Artrith"
DS_DESC = "Approximately 6,900 configurations of bulk water, water clusters\
 and Li8Mo2Ni7Ti7O32 used in the training of an ANN, whereby total energy\
 is extrapolated by a Taylor expansion as a means of reducing computational\
 costs."
ELEMENTS = ["H", "O", "Li", "Mo", "Ni", "Ti"]
GLOB_STR = "*.xsf"

E_RE = re.compile(r"# total energy = (\S+)( eV)?$")


def get_method_name(filepath):
    if "LMNTO-SCAN" in filepath.parts[-2]:
        return ("SCAN", filepath.parts[-2])
    elif "water-clusters" in filepath.parts[-3]:
        return ("DFT-BLYP-D3/def2-TZVP", filepath.parts[-3])
    elif "liquid-64water" in filepath.parts[-2]:
        return ("DFT-revPBE+D3", filepath.parts[-2])
    else:
        return None


def reader(filepath):
    methods, name = get_method_name(filepath)
    config = read(filepath)
    config.info["methods"] = methods
    config.info["name"] = name
    with open(filepath, "r") as f:
        match = E_RE.match(f.readline())
    config.info["energy"] = float(match.groups()[0])
    config.info["forces"] = config.get_forces()
    return [config]


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

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
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_regexes = [
        [
            f"{DATASET}-LiMoNiTi-train",
            "LMNTO-SCAN-train-data",
            f"Training configurations of Li8Mo2Ni7Ti7O32 from {DATASET} dataset",
        ],
        [
            f"{DATASET}-LiMoNiTi-validation",
            "LMNTO-SCAN-validation-data",
            f"Validation configurations of Li8Mo2Ni7Ti7O32 from {DATASET} dataset",
        ],
        [
            f"{DATASET}-bulk-water-train-test",
            "liquid-64water-AIMD-RPBE-D3-train-test-data",
            f"Training and testing configurations of bulk water from {DATASET} dataset",
        ],
        [
            f"{DATASET}-bulk-water-validation",
            "liquid-64water-AIMD-RPBE-D3-validation-data",
            f"Validation configurations of bulk water from {DATASET} dataset",
        ],
        [
            f"{DATASET}-water-clusters",
            "water-clusters-BLYP-D3",
            f"Configurations of water clusters from {DATASET} dataset",
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
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        pr_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

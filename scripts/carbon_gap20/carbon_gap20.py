"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.17863/CAM.54529
Download link:
https://www.repository.cam.ac.uk/bitstream/handle/1810/307452/Carbon_GAP_20.tgz?sequence=1&isAllowed=y

Extract to project folder
tar -xzf Carbon_GAP_20.tgz -C $project_dir/scripts/carbon_gap20/ \
    Carbon_GAP_20/Carbon_Data_Set_Total.xyz \
    Carbon_GAP_20/Carbon_GAP_20_Training_Set.xyz

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------
virial
cutoff
config_type
nneightol
kpoints
kpoints_density

File notes
----------
keys in training set xyz comment line
energy config_type virial cutoff nneightol pbc Lattice Properties=species:S:1:pos:R:3:Z:I:1:force:R:3
keys in all configs xyz comment line
energy config_type kpoints kpoints_density virial cutoff nneightol pbc Lattice Properties=species:S:1:pos:R:3:Z:I:1:force:R:3


"""
from ase.io import read
from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("scripts/carbon_gap20")
DATASET = "Carbon-GAP20"

SOFTWARE = "VASP"
METHODS = "DFT optB88-vdW"
LINKS = [
    "https://doi.org/10.17863/CAM.54529",
    "https://doi.org/10.1063/5.0005084",
]
AUTHORS = "G. Csanyi"
DS_DESC = "Approximately 17,000 configurations of carbon, each containing 1 to\
 240 atoms/cell, including a subset of 6577 configurations used for training.\
 A variety of structure types are represented, including graphite, graphene,\
 cubic and hexagonal diamond, fullerenes, and nanotubes, as well as some\
 defect structures."

ELEMENTS = ["C"]
GLOB_STR = "*.xyz"


def reader(filepath):
    configs = read(filepath, index=":")
    for config in configs:
        config.info["name"] = filepath.stem
    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")

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
        "virial": {"field": "virial"},
        "kpoints_density": {"field": "kpoints_density"},
        "cutoff": {"field": "cutoff"},
        "config_type": {"field": "config_type"},
        "nneightol": {"field": "nneightol"},
        "kpoints": {"field": "kpoints"},
        # "": {"field": ""},
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
                "forces": {"field": "force", "units": "eV/A"},
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
            DATASET,
            ".*",
            f"All configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-training-set",
            ".*Training_Set",
            f"Training set configurations from {DATASET} dataset",
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
        cs_ids,
        all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

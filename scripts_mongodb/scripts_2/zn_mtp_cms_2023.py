"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

# from ase.io import read

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/zn_mtp_cms2023")
DATASET_NAME = "Zn_MTP_CMS2023"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1016/j.commatsci.2023.112723"
DATA_LINK = "https://github.com/meihaojie/Zn_system/tree/main"
# OTHER_LINKS = []

AUTHORS = [
    "Haojie Mei",
    "Luyao Cheng",
    "Liang Chen",
    "Feifei Wang",
    "Jinfu Li",
    "Lingti Kong",
]
DATASET_DESC = (
    "A training dataset of diverse atomic configurations of Zn, varying in "
    "aggregation states, crystal structures, defect types, and sizes. The aim was "
    "to derive a potential capable of accurately describing a broad spectrum of "
    "local atomic configurations in Zn."
)
ELEMENTS = None
GLOB_STR = "Zn_cfg"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "encut": {"value": 450, "units": "eV"},
            "kpoint-scheme": "gamma-centered Monkhorst-Pack",
            "ediff": {"value": 10e-7, "units": "eV"},
            "ediffg": {"value": 10e-2, "units": "eV/angstrom"},
        }
    },
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
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/angstrom^3"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


SYMBOL_DICT = {"0": "Zn"}


def convert_stress(keys, stress):
    stresses = {k: s for k, s in zip(keys, stress)}
    return [
        [stresses["xx"], stresses["xy"], stresses["xz"]],
        [stresses["xy"], stresses["yy"], stresses["yz"]],
        [stresses["xz"], stresses["yz"], stresses["zz"]],
    ]


def reader(filepath):
    with open(filepath, "rt") as f:
        energy = None
        forces = None
        coords = []
        cell = []
        symbols = []
        config_count = 0
        for line in f:
            if line.strip().startswith("Size"):
                size = int(f.readline().strip())
            elif line.strip().lower().startswith("supercell"):
                cell.append([float(x) for x in f.readline().strip().split()])
                cell.append([float(x) for x in f.readline().strip().split()])
                cell.append([float(x) for x in f.readline().strip().split()])
            elif line.strip().startswith("Energy"):
                energy = float(f.readline().strip())
            elif line.strip().startswith("PlusStress"):
                stress_keys = line.strip().split()[-6:]
                stress = [float(x) for x in f.readline().strip().split()]
                stress = convert_stress(stress_keys, stress)
            elif line.strip().startswith("AtomData:"):
                keys = line.strip().split()[1:]
                if "fx" in keys:
                    forces = []
                for i in range(size):
                    li = {
                        key: val for key, val in zip(keys, f.readline().strip().split())
                    }
                    symbols.append(SYMBOL_DICT[li["type"]])
                    if "cartes_x" in keys:
                        coords.append(
                            [
                                float(c)
                                for c in [
                                    li["cartes_x"],
                                    li["cartes_y"],
                                    li["cartes_z"],
                                ]
                            ]
                        )
                    elif "direct_x" in keys:
                        coords.append(
                            [
                                float(c)
                                for c in [
                                    li["direct_x"],
                                    li["direct_y"],
                                    li["direct_z"],
                                ]
                            ]
                        )

                    if "fx" in keys:
                        forces.append(
                            [float(f) for f in [li["fx"], li["fy"], li["fz"]]]
                        )

            elif line.startswith("END_CFG"):
                if "cartes_x" in keys:
                    config = AtomicConfiguration(
                        positions=coords, symbols=symbols, cell=cell
                    )
                elif "direct_x" in keys:
                    config = AtomicConfiguration(
                        scaled_positions=coords, symbols=symbols, cell=cell
                    )
                config.info["energy"] = energy
                if forces:
                    config.info["forces"] = forces
                config.info["stress"] = stress  # Stress units appear to be GPa
                config.info["name"] = f"zn_mtp_cms2023_{config_count}"
                config_count += 1
                yield config
                forces = None
                stress = []
                coords = []
                cell = []
                symbols = []
                energy = None


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
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

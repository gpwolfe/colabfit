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
files formatted with the MLIP package: https://mlip.skoltech.ru/
manual can be viewed online here:
https://gitlab.com/ashapeev/mlip-2-paper-supp-info/-/blob/master/manual.pdf
The reader function is also saved in colabfit-utilities
"""
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/zr_sn_jnm_2024")
DATASET_NAME = "Zr_Sn_JNM_2024"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1016/j.jnucmat.2023.154794"
DATA_LINK = "https://github.com/meihaojie/Zr_Sn_system"
LINKS = [
    "https://github.com/meihaojie/Zr_Sn_system",
    "https://doi.org/10.1016/j.jnucmat.2023.154794",
]
AUTHORS = [
    "Haojie Mei",
    "Liang Chen",
    "Feifei Wang",
    "Guisen Liu",
    "Jing Hu",
    "Weitong Lin",
    "Yao Shen",
    "Jinfu Li",
    "Lingti Kong",
]
DATASET_DESC = (
    "This dataset contains data from density functional theory calculations "
    "of various atomic configurations of pure Zr, pure Sn, and Zr-Sn alloys "
    "with different structures, defects, and compositions. Energies, "
    "forces, and stresses are calculated at the DFT level of theory. "
    "Includes 23,956 total configurations."
)
ELEMENTS = ["Zr", "Sn"]
GLOB_STR = "ZrSn.cfg"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "input": {
        "value": {
            "kpoint-scheme": "gamma-centered",
            "encut": {"value": 400, "units": "eV"},
            "ediff": 10e-6,
            "ediffg": 10e-2,
            "kspacing": {"value": 0.03, "units": "2*pi/Ang"},
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
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


CSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]

SYMBOL_DICT = {"0": "Zr", "1": "Sn"}


def convert_stress(keys, stress):
    stresses = {k: s for k, s in zip(keys, stress)}
    return [
        [stresses["xx"], stresses["xy"], stresses["xz"]],
        [stresses["xy"], stresses["yy"], stresses["yz"]],
        [stresses["xz"], stresses["yz"], stresses["zz"]],
    ]


def reader(filepath):
    # mtp_stress_order = ["xx", "yy", "zz", "yz", "xz", "xy"]
    # vasp_stress_order = ["xx", "yy", "zz", "xy", "yz", "xz"]

    # configs = []
    with open(filepath, "rt") as f:
        energy = None
        forces = []
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
                config.info["forces"] = forces
                config.info["stress"] = stress
                config.info["name"] = f"{filepath.stem}_{config_count}"
                config_count += 1
                yield config
                forces = []
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
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DATASET_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

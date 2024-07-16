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


DATASET_FP = Path("data/mtpu_2023")
DATASET_NAME = "MTPu_2023"
# LICENSE = "" # no license listed

PUBLICATION = "https://doi.org/10.48550/arXiv.2311.15170"
DATA_LINK = "https://gitlab.com/Kazongogit/MTPu"
# OTHER_LINKS = []

AUTHORS = [
    "Karim Zongo",
    "Hao Sun",
    "Claudiane Ouellet-Plamondon",
    "Laurent Karim Beland",
]
DATASET_DESC = (
    "A comprehensive database generated using density functional "
    "theory simulations, encompassing a wide range of crystal structures, "
    "point defects, extended defects, and disordered structure."
)
ELEMENTS = ["Si", "O"]
GLOB_STR = "*.cfg"

PI_METADATA = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE"},
    "input": {"field": "input"},
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
            "stress": {"field": "stress", "units": "GPa"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


CSS = [
    [
        f"{DATASET_NAME}_Si",
        {"names": {"$regex": "_Si_"}},
        f"Configurations of Si from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_SiO2",
        {"names": {"$regex": "_SiO2_"}},
        f"Configurations of SiO2 from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_O",
        {"names": {"$regex": "_O_"}},
        f"Configurations of oxygen from {DATASET_NAME} dataset",
    ],
]


SYMBOL_DICT = {"0": "Si", "1": "O"}


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
                config.info["stress"] = stress

                if "Si" in symbols and "O" in symbols:
                    config.info["input"] = {
                        "kpoint-scheme": "Monkhorst-Pack",
                        "kpoints": "11x11x11",
                        "kinetic-energy-cutoff": {
                            "val": 1224,
                            "units": "eV",
                        },
                    }
                    config.info["name"] = f"{filepath.stem}_SiO2_{config_count}"
                elif "Si" in symbols:
                    config.info["input"] = {
                        "kpoint-scheme": "Monkhorst-Pack",
                        "kpoints": "8x8x8",
                        "kinetic-energy-cutoff": {
                            "val": 884,
                            "units": "eV",
                        },
                    }
                    config.info["name"] = f"{filepath.stem}_Si_{config_count}"
                elif "O" in symbols:
                    config.info["input"] = {
                        "kpoint-scheme": "Monkhorst-Pack",
                        "kpoints": "gamma-point",
                        "kinetic-energy-cutoff": {
                            "val": 1224,
                            "units": "eV",
                        },
                    }
                    config.info["name"] = f"{filepath.stem}_O_{config_count}"
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
            # co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=False,
            # data_license= # No license listed
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
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        # data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

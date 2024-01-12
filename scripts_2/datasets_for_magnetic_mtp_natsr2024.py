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

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/datasets_for_magnetic_mtp_natsr2024")
DATASET_NAME = "datasets_for_magnetic_MTP_NatSR2024"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1038/s41598-023-46951-x"
DATA_LINK = "https://gitlab.com/ivannovikov/datasets_for_magnetic_MTP"
# OTHER_LINKS = []

AUTHORS = [
    "Alexey S. Kotykhov",
    "Konstantin Gubaev",
    "Max Hodapp",
    "Christian Tantardini",
    "Alexander V. Shapeev",
    "Ivan S. Novikov",
]
DATASET_DESC = ""
ELEMENTS = ["Al", "Fe"]

PI_METADATA = {
    "software": {"value": "ABINIT"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "k-point": "6x6x6",
            "cutoff-energy": {"value": 25, "units": "hartree"},
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
            "stress": {"field": "stress", "units": "GPa"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "magnetic-moment": {"field": "magmom"},
}


DSS = [
    (
        f"{DATASET_NAME}_training",
        "training_set.cfg",
        "This dataset comprises a training dataset for magnetic multi-component "
        "machine-learning potentials for Fe-Al systems, including different "
        "concentrations of Fe and Al (Al concentrations from 0%-50%), with fully "
        "equilibrated and perturbed atomic positions, lattice vectors and magnetic "
        "moments represented.",
    ),
    (
        f"{DATASET_NAME}_verification",
        "verification_set.cfg",
        "This is the verification dataset (see companion training dataset: "
        f"{DATASET_NAME}_training) used in training a magnetic multi-component "
        "machine-learning potential for Fe-Al systems. The configurations from the "
        "verification set include different levels of magnetic moment perturbation "
        "than configurations from the training set. For this reason, the authors "
        'refer to this dataset as a "verification set", rather than a '
        '"validation set".',
    ),
]

SYMBOL_DICT = {"0": "Al", "1": "Fe"}


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
        magmom = None
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
                if "magmom_x" in keys:
                    magmom = []
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
                    if "magmom_x" in keys:
                        magmom.append(
                            [
                                float(m)
                                for m in [
                                    li["magmom_x"],
                                    li["magmom_y"],
                                    li["magmom_z"],
                                ]
                            ]
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
                if magmom:
                    config.info["magmom"] = magmom
                config.info["stress"] = stress  # Stress units appear to be GPa
                symbolset = "".join(sorted(list(set(symbols))))
                config.info["name"] = f"{filepath.stem}_{symbolset}_{config_count}"
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

    for i, (ds_name, ds_glob, ds_desc) in enumerate(DSS):
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=ds_glob,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))
        css = [
            [
                f"{ds_name}_Fe",
                {"names": {"$regex": "_Fe_"}},
                f"Fe-only configurations from {ds_name}",
            ],
            [
                f"{ds_name}_Fe_Al",
                {"names": {"$regex": "_AlFe_"}},
                f"Fe-Al configurations from {ds_name}, "
                "in which the concentration of Al varies from 0%-50%",
            ],
        ]
        cs_ids = []
        for i, (name, query, desc) in enumerate(css):
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
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
            description=ds_desc,
            verbose=False,
            cs_ids=cs_ids,  # remove line if no configuration sets to insert
            data_license=LICENSE,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

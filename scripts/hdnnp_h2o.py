"""
author:gpwolfe

Data can be downloaded from:

Download link:

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
format of input file for ruNNer here:
https://theochemgoettingen.gitlab.io/RuNNer/1.3/reference/files/#inputdata

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/hdnnp_h2o_np"
    "/training-data_H2O"
)
DATASET = "HDNNP_H2O"

SOFTWARE = "FHI-aims"
PUBLICATION = "https://doi.org/10.1073/pnas.1602375113"
DATA_LINK = "https://doi.org/10.5281/zenodo.2634097"
OTHER_LINKS = "https://doi.org/10.1103/PhysRevLett.98.146401"
LINKS = [
    "https://doi.org/10.5281/zenodo.2634097",
    "https://doi.org/10.1103/PhysRevLett.98.146401",
    "https://doi.org/10.1073/pnas.1602375113",
]
AUTHORS = ["Tobias Morawietz", "Jörg Behler"]
DS_DESC = "Approximately 28,000 configurations split into 4 datasets, each\
 using a different functional, used in the training of a high-dimensional\
 neural network potential (HDNNP). "
ELEMENTS = ["H", "O"]
GLOB_STR = "input.data*"


def reader(filepath):
    with open(filepath, "r") as f:
        configs = []
        cell = []
        positions = []
        forces = []
        atomic_charges = []
        symbols = []
        for line in f:
            if line.strip() == "begin":
                pass
            elif line.startswith("energy"):
                energy = float(line.split()[1])
            elif line.startswith("charge"):
                charge = float(line.split()[1])
            elif line.strip() == "end":
                config = AtomicConfiguration(positions=positions, symbols=symbols)
                positions = []
                symbols = []
                config.cell = cell
                cell = []
                config.info["forces"] = forces
                forces = []
                config.info["atomic_charges"] = atomic_charges
                atomic_charges = []
                config.info["energy"] = energy
                config.info["charge"] = charge
                config.info["name"] = filepath.name
                config.info["method"] = f"DFT-{filepath.name.split('.')[-1]}"

                configs.append(config)
            elif line.startswith("atom"):
                line_splt = line.split()
                positions.append([float(x) for x in line_splt[1:4]])
                symbols.append(line_splt[4])
                atomic_charges.append(float(line_splt[5]))
                forces.append([float(x) for x in line_splt[7:]])
            elif line.startswith("comment"):
                pass
            elif line.startswith("lattice"):
                cell.append([float(x) for x in line.split()[1:]])
            else:
                print(f"error at {line}")
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
        "method": {"field": "method"},
    }
    co_md_map = {
        "atomic-charges": {"field": "atomic_charges"},
        "charge": {"field": "charge"},
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
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            f"{DATASET}_BLYP",
            r"input\.data\.BLYP",
            f"All configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}_BLYP_D3",
            r"input\.data\.BLYP_D3",
            f"All configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}_RPBE",
            r"input\.data\.RPBE",
            f"All configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}_RPBE_D3",
            r"input\.data\.RPBE_D3",
            f"All configurations from {DATASET} dataset",
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
                co_ids, ds_id=ds_id, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

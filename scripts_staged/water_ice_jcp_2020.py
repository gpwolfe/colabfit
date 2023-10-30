"""
author: Gregory Wolfe

Properties
----------
energy
forces

Other properties added to metadata
----------------------------------

File notes
----------
n2p2 file format
There are two training input.data files and a number of test data files
of format ex. "q_wat_350k.data". There are other ".data" files that are not
of interest in the directory.

"""
from argparse import ArgumentParser
from pathlib import Path
import re
import sys

# from ase.io import read

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/MarsalekGroup-water-ice-jcp-2020")
DATASET_NAME = "water_ice_JCP_2020"

SOFTWARE = "CP2K"
METHODS = "DFT-revPBE0-D3"

PUBLICATION = "https://doi.org/10.1063/5.0016004"
DATA_LINK = "https://doi.org/10.5281/zenodo.4004590"
LINKS = ["https://doi.org/10.5281/zenodo.4004590", "https://doi.org/10.1063/5.0016004"]
AUTHORS = ["Christoph Schran", "Kyrstof Brezina", "Ondrej Marsalek"]
DATASET_DESC = (
    "Starting from a single reference ab initio simulation, we use active "
    "learning to expand into new state points and to describe the quantum "
    "nature of the nuclei. The final model, trained on 814 reference calculations, "
    "yields excellent results under a range of conditions, from liquid water at "
    "ambient and elevated temperatures and pressures to different phases of ice, "
    "and the air-water interface — all including nuclear quantum effects."
)
ELEMENTS = None


ATOM_RE = re.compile(
    r"atom\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+"
    r"(?P<z>\-?\d+\.\d+)\s+(?P<element>\w{1,2})\s+0.0+\s+0.0+\s+(?P<f1>\-?\d+\.\d+)"
    r"\s+(?P<f2>\-?\d+\.\d+)\s+(?P<f3>\-?\d+\.\d+)"
)
LATT_RE = re.compile(
    r"lattice\s+(?P<lat1>\-?\d+\.\d+)\s+(?P<lat2>\-?\d+\.\d+)\s+(?P<lat3>\-?\d+\.\d+)"
)
EN_RE = re.compile(r"energy\s+(?P<energy>\-?\d+\.\d+)")


# Assign additional relevant property instance metadata, such as basis set used
PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    # "basis-set": {"field": "basis_set"}
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
}

CO_METADATA = {
    "enthalpy": {"field": "h", "units": "Ha"},
    "zpve": {"field": "zpve", "units": "Ha"},
}

CSS = [
    (
        "water_ice_jcp_2020_generation_1_training_set",
        DATASET_FP / "generation-1/training-set",
        {"names": {"$regex": "generation-1"}},
        "Configurations from generation 1 of the C-NNP model training process from "
        "water_ice_JCP_2020",
    ),
    (
        "water_ice_jcp_2020_generation_4_training_set",
        DATASET_FP / "generation-4/training-set",
        {"names": {"$regex": "generation-4"}},
        "Configurations from generation 4 of the C-NNP model training process from "
        "water_ice_JCP_2020",
    ),
    (
        "water_ice_jcp_2020_test_set",
        DATASET_FP / "test-set",
        {"names": {"$regex": "test-set"}},
        "Configurations from the test set for the C-NNP model from water_ice_JCP_2020",
    ),
]


def reader(filepath):
    with open(filepath) as f:
        configurations = []
        lattice = []
        coords = []
        forces = []
        elements = []
        counter = 0
        for line in f:
            if (
                line.startswith("begin")
                # or line.startswith("end")
                or line.startswith("charge")
                or line.startswith("comment")
            ):
                pass
            elif line.startswith("lattice"):
                lattice.append([float(x) for x in LATT_RE.match(line).groups()])
            elif line.startswith("atom"):
                ln_match = ATOM_RE.match(line)
                coords.append([float(x) for x in ln_match.groups()[:3]])
                forces.append([float(x) for x in ln_match.groups()[-3:]])
                elements.append(ln_match.groups()[3])
            elif line.startswith("energy"):
                energy = float(EN_RE.match(line).group("energy"))
            elif line.startswith("end"):
                config = AtomicConfiguration(
                    positions=coords, symbols=elements, cell=lattice
                )
                config.info["forces"] = forces
                config.info["energy"] = energy
                config.info[
                    "name"
                ] = f"{filepath.parts[-3]}_{filepath.parts[-2]}_{counter}"
                configurations.append(config)
                # if counter == 100:  # comment after testing
                #     return configurations  # comment after testing
                counter += 1
                lattice = []
                coords = []
                forces = []
                elements = []
    return configurations


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = []
    cs_ids = []
    for i, (name, path, query, desc) in enumerate(CSS):
        configurations = load_data(
            file_path=path,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string="*.data",
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
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

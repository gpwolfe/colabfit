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
import re
import sys

# from ase.io import read

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/phosphorene_dataset")
DATASET_NAME = "defected_phosphorene_ACS_2023"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1021/acs.jpcc.3c05713"
DATA_LINK = "https://doi.org/10.5281/zenodo.8421094"

AUTHORS = ["Lukáš Kývala", "Andrea Angeletti", "Cesare Franchini", "Christoph Dellago"]
DATASET_DESC = (
    "This dataset contains pristine monolayer phosphorene as well as "
    "structures with monovacancies which were used to train an artificial neural "
    "network (ANN) for use with a high-dimensional neural network potentials "
    "molecular dynamics (HDNNP-MD) simulation. The publication investigates the "
    "mechanism and rates of the processes of defect diffusion, as well as "
    "monovacancy-to-divacancy defect coalescence."
)
ELEMENTS = None
GLOB_STR = "*.data"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "ENCUT": {"value": 600, "unit": "eV"},
            "EDIFFG": {"value": 0.01, "unit": "eV/A"},
            "KPOINTS": "3x3x3",
            "kpoints-scheme": "gamma-centered",
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
}


ATOM_RE = re.compile(
    r"atom\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+"
    r"(?P<z>\-?\d+\.\d+)\s+(?P<element>\w{1,2})\s+0.0+\s+0.0+\s+(?P<f1>\-?\d+\.\d+)"
    r"\s+(?P<f2>\-?\d+\.\d+)\s+(?P<f3>\-?\d+\.\d+)"
)
LATT_RE = re.compile(
    r"lattice\s+(?P<lat1>\-?\d+\.\d+)\s+(?P<lat2>\-?\d+\.\d+)\s+(?P<lat3>\-?\d+\.\d+)"
)

EN_RE = re.compile(r"energy\s+(?P<energy>\-?\d+\.\d+)")


def n2p2_reader(filepath):
    with open(filepath) as f:
        configurations = []
        lattice = []
        coords = []
        forces = []
        elements = []
        counter = 0
        for line in f:
            if line.startswith("begin") or line.startswith("charge"):
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
                config.info["name"] = f"{filepath.parts[-2]}_{counter}"
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=n2p2_reader,
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

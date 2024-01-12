"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.5281/zenodo.7018572
Download links:
https://zenodo.org/record/7018573/files/nep.in?download=1
https://zenodo.org/record/7018573/files/nep.txt?download=1
https://zenodo.org/record/7018573/files/test.in?download=1
https://zenodo.org/record/7018573/files/train.in?download=1

Move to script folder

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
Potential energy
forces

Other properties added to metadata
----------------------------------
virial (6-vector)


File notes
----------
energy, virial?
-1047.855796 23.6717 9.39515 69.8057 -10.89163 8.26454 -2.84489
lattice
1.63123700e+01 0.00000000e+00 0.00000000e+00 0.00000000e+00 9.37147200e+00 \
    0.00000000e+00 0.00000000e+00 0.00000000e+00 3.43319720e+01
element coordinates force
C   1.32592800e+01  2.37400000e-01  1.14220900e+01 -5.76856000e-01 \
    -2.09074000e-01 -1.31760100e+00

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
import re
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/nep_qhpf/data"
)
DATASET_FP = Path().cwd().parent / "data/nep_qhpf"
DATASET = "NEP_qHPF"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"
PUBLICATION = "https://doi.org/10.1016/j.eml.2022.101929"
DATA_LINK = "https://doi.org/10.5281/zenodo.7018572"
LINKS = [
    "https://doi.org/10.5281/zenodo.7018572",
    "https://doi.org/10.1016/j.eml.2022.101929",
]
AUTHORS = "Penghua Ying"
ELEMENTS = ["C"]
GLOB_STR = "*.in"

EN_V_RE = re.compile(
    r"^([-\.\de]+)\s([-\.\de]+)\s([-\.\de]+)\s"
    r"([-\.\de]+)\s([-\.\de]+)\s([-\.\de]+)\s([-\.\de]+)$"
)
CO_FO_RE = re.compile(r"^(\S)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$")
LATT_RE = re.compile(r"^(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)$")


def reader(filepath):
    counter = 1
    config_no = 0
    configs = []
    energy = None
    virial = []
    force = []
    coords = []
    cell = []

    with open(filepath, "r") as f:
        for line in f:
            if counter == 1:
                num_a = int(line.rstrip())
                counter += 1
            elif counter < num_a:
                counter += 1
                pass
            else:
                if line.startswith("C"):
                    match = CO_FO_RE.match(line)
                    coords.append([float(x) for x in match.groups()[1:4]])
                    force.append([float(x) for x in match.groups()[4:]])

                elif len(line.split()) == 7:
                    # check whether any data has been gathered yet
                    if energy:
                        config = AtomicConfiguration(
                            positions=coords,
                            symbols=["C" for x in coords],
                            cell=cell,
                        )
                        config.info["virial"] = [
                            [virial[0], virial[5], virial[4]],
                            [virial[5], virial[1], virial[3]],
                            [virial[4], virial[3], virial[2]],
                        ]
                        config.info["energy"] = energy
                        config.info["name"] = f"{filepath.stem}_{config_no}"
                        config.info["forces"] = force
                        config_no += 1
                        configs.append(config)
                        force = []
                        coords = []
                    match = EN_V_RE.match(line)
                    energy = float(match.groups()[0])
                    virial = [float(x) for x in match.groups()[1:]]
                elif len(line.split()) == 9:
                    match = LATT_RE.match(line)
                    cell = np.array([float(x) for x in match.groups()[:]])
                    cell = cell.reshape(3, 3)

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

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {"encut": {"value": 520, "units": "eV"}},
            "kspacing": {"value": 0.25, "units": "Ang^-1"},
        },
    }
    # co_md_map = {
    #     "virial": {"field": "virial"},
    # }
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
                "forces": {"field": "forces", "units": "eV/angstrom"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV/atom"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }

    for glob_ds in ["test", "train"]:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=f"{glob_ds}.in",
            generator=False,
        )
        ds_id = generate_ds_id()
        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=property_map,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))
        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=f"{DATASET}_{glob_ds}",
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=(
                f"The {glob_ds} set of a train and test set pair."
                "The combined datasets comprise approximately 275 configurations "
                "of monolayer quasi-hexagonal-phase fullerene (qHPF) membrane used "
                "to train and test an NEP model."
            ),
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

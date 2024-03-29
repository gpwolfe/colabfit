"""
author:gpwolfe

Data can be downloaded from:
https://github.com/mengfsou/NNIP-FeH
Download link:
https://github.com/mengfsou/NNIP-FeH/archive/refs/heads/main.zip

unzip and extract to project folder
unzip NNIP-FeH-main.zip "**/*.tar*" -d <project_dir>/scripts/nnip
cat <project_dir>/scripts/nnip/NNIP-FeH-main/DATABASE/database.tar.gz* | tar \
    -zxv -C <project_dir>scripts/nnip/NNIP-FeH-main/DATABASE/

** THE ABOVE LINE IS REQUIRED TO PROPERLY CONSTRUCT DATABASE **
** A NORMAL TAR COMMAND WILL RESULT IN THREE INCOMPLETE DATABASES **

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
None

File notes
----------
For database format details, see:
https://compphysvienna.github.io/n2p2/topics/cfg_file.html
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import re
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/nnip")
DATASET_FP = Path().cwd().parent / "data/nnip"  # local
DATASET = "NNIP_FeH_PRM_2021"
PUBLICATION = "https://doi.org/10.1103/PhysRevMaterials.5.113606"
DATA_LINK = "https://github.com/mengfsou/NNIP-FeH"
LINKS = [
    "https://github.com/mengfsou/NNIP-FeH",
    "https://doi.org/10.1103/PhysRevMaterials.5.113606",
]
SOFTWARE = "VASP"
METHODS = "DFT-PBE"

ATOM_RE = re.compile(
    r"atom\s+(\-?\d+\.\d+)\s+(\-?\d+\.\d+)\s+"
    r"(\-?\d+\.\d+)\s+(\w{1,2})\s+0.0+\s+0.0+\s+(\-?\d+\.\d+)"
    r"\s+(\-?\d+\.\d+)\s+(\-?\d+\.\d+)"
)
LATT_RE = re.compile(r"lattice\s+(\-?\d+\.\d+)\s+(\-?\d+\.\d+)\s+(\-?\d+\.\d+)")
EN_RE = re.compile(r"energy\s+(\-?\d+\.\d+)")


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
                or line.startswith("end")
                or line.startswith("charge")
                or line.startswith("comment")
            ):
                pass
            elif line.startswith("lattice"):
                lattice.append([float(x) for x in LATT_RE.match(line).groups()])
            elif line.startswith("atom"):
                ln_match = ATOM_RE.match(line)
                coords.append([float(x) for x in ln_match.groups()[0:3]])
                forces.append([float(x) for x in ln_match.groups()[-3:]])
                elements.append(ln_match.groups()[3])
            elif line.startswith("energy"):
                energy = float(EN_RE.match(line).groups()[0])
                config = AtomicConfiguration(
                    positions=coords, symbols=elements, cell=lattice
                )
                config.info["forces"] = forces
                config.info["energy"] = energy
                config.info["name"] = f"NNIP_FeH_{counter}"
                configurations.append(config)
                # if counter == 100:  # remove after testing
                #     return configurations # remove after testing
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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Fe", "H"],
        reader=reader,
        glob_string="database.data",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {
                "encut": {"value": 360, "units": "eV"},
                "kspacing": {"value": 0.03, "units": "Ang^-1"},
                "sigma": 0.1,
                "nelm": {"value": 10e-5, "units": "eV"},
            }
        },
    }
    property_map = {
        # According to N2P2 docs, energy value represents "total potential
        # energy".
        # see: https://compphysvienna.github.io/n2p2/topics/cfg_file.html
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
    }
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
    cs_regexes = [
        [
            f"{DATASET}_Fe",
            ["Fe"],
            f"alpha-iron-only configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}_H",
            ["H"],
            f"Hydrogen-only configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}_Fe",
            ["Fe", "H"],
            "Configurations containing alpha-iron with hydrogen "
            f"from {DATASET} dataset",
        ],
    ]

    cs_ids = []

    for i, (name, elem, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"elements": {"$eq": elem}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DATASET,
        authors=[
            "Fan-Shun Meng",
            "Jun-Ping Du",
            "Shuhei Shinzato",
            "Hideki Mori",
            "Peijun Yu",
            "Kazuki Matsubara",
            "Nobuyuki Ishikawa",
            "Shigenobu Ogata",
        ],
        links=[PUBLICATION, DATA_LINK],
        description="Approximately 20,000 configurations from a dataset of "
        "alpha-iron and hydrogen. Properties include forces and potential "
        "energy, calculated using VASP at the DFT level using the GGA-PBE "
        "functional.",
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

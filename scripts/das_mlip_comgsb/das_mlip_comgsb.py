"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.1103/PhysRevB.104.094310
Download link:
https://journals.aps.org/prb/supplemental/10.1103/PhysRevB.104.094310/training_dataset.zip


Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
force
energy

Other properties added to metadata
----------------------------------
virial


File notes
----------
virial is a 6-vector, not a 9-vector

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
    # cauchy_stress_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("").cwd()
DATASET = "DAS_ML-IP_CoSb_MgSb"

SOFTWARE = "VASP, LAMMPS"
METHODS = "DFT-PBE"
LINKS = ["https://doi.org/10.1103/PhysRevB.104.094310"]
AUTHORS = [
    "Hongliang Yang",
    "Yifan Zhu",
    "Erting Dong",
    "Yabei Wu",
    "Jiong Yang",
    "Wenqing Zhang",
]
DS_DESC = "Approximately 850 configurations of CoSb3 and Mg3Sb2 generated\
 using a dual adaptive sampling (DAS) method for use with machine learning\
 of interatomic potentials (MLIP)."
ELEMENTS = ["Mg", "Co", "Sb"]
GLOB_STR = "training_dataset*Sb*.cfg"


def reader(filepath):

    mtp_stress_order = ["xx", "yy", "zz", "yz", "xz", "xy"]
    vasp_stress_order = ["xx", "yy", "zz", "xy", "yz", "xz"]
    if "Mg3Sb" in filepath.stem:
        symbol_dict = {0: "Mg", 1: "Sb"}
    elif "CoSb" in filepath.stem:
        symbol_dict = {0: "Mg", 1: "Sb"}
    else:
        print(f"Did not recognize elements in file name {filepath.stem}")
    configs = []
    with open(filepath, "rt") as f:
        energy = None
        forces = []
        coords = []
        cell = []
        symbols = []
        config_count = 0
        for line in f:
            if line.startswith("SuperCell"):
                cell.append([float(x) for x in f.readline().strip().split()])
                cell.append([float(x) for x in f.readline().strip().split()])
                cell.append([float(x) for x in f.readline().strip().split()])
            elif line.startswith("Energy"):
                energy = float(f.readline().strip())
            elif line.startswith("PlusStress"):
                virial = [float(x) for x in f.readline().strip().split()]
                virial = [
                    virial[mtp_stress_order.index(n)]
                    for n in vasp_stress_order
                ]
            elif len(line.strip().split()) == 8:
                li = line.strip().split()
                symbols.append(symbol_dict[int(li[1])])
                forces.append(li[5:])
                coords.append(li[2:5])

            elif line.startswith("END_CFG"):
                config = AtomicConfiguration(
                    positions=coords, symbols=symbols, cell=cell
                )
                config.info["energy"] = energy
                config.info["forces"] = forces
                config.info["virial"] = virial
                config.info["name"] = f"{filepath.stem}_{config_count}"
                config_count += 1
                configs.append(config)
                forces = []
                virial = []
                coords = []
                cell = []
                symbols = []
                energy = None

    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
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
    # client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "virial": {"field": "virial"}
        # "": {"field": ""}
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
        # "cauchy-stress": [
        #     {
        #         "stress": {"field": "virial", "units": "eV"},
        #         "_metadata": metadata,
        #     }
        # ],
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
            f"{DATASET}-CoSb",
            ".*CoSb*",
            f"CoSb3 configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-MgSb",
            ".*Mg3Sb*",
            f"Mg3Sb2 configurations from {DATASET} dataset",
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
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

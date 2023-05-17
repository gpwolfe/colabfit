"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2021.153

https://archive.materialscloud.org/record/file?filename=brass_DFT_data.zip&record_id=1011

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
None

File notes
----------
"""

from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from collections import defaultdict
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("brass_DFT_data/brass_data")


def read_npz(filepath):
    # print(filepath)
    data = defaultdict(list)
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
    return data


def reader(filepath):
    name = "alpha-brass-nanoparticles"
    data = read_npz(filepath)
    old_keys = (
        "coords",
        "latt",
        "z",
        "F",
        "E",
        "E_coh",
        "comp",
        "cmts",
        "theory",
        "name",
        "citation",
    )

    new_keys = (
        "coords",
        "lattice",
        "atomic_num",
        "forces",
        "total_energy",
        "cohesive_energy",
        "composition_dict",
        "comments",
        "vasp_pbe",
        "citation",
    )
    for old, new in zip(old_keys, new_keys):
        data[new] = data.pop(old)

    atoms = [
        AtomicConfiguration(
            names=[name],
            positions=data["coords"][i],
            cell=data["lattice"][i],
            numbers=data["atomic_num"][i],
            pbc=True,
        )
        for i, val in enumerate(data["coords"])
    ]
    using_keys = ("forces", "total_energy")
    for i, atom in enumerate(atoms):
        for key in using_keys:
            atom.info[key] = data[key][i]
        atom.info["name"] = name
    return atoms


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
        # Data can be downloaded here:
        # 'https://archive.materialscloud.org/record/2021.153'
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Cu", "Zn"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    pds = [atomic_forces_pd, potential_energy_pd]
    for pd in pds:
        client.insert_property_definition(pd)
    metadata = {
        "software": {"value": "VASP"},
        "method": {"value": "DFT-PBE"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "total_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "meV Å^-1"},
                "_metadata": metadata,
            }
        ],
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

    name = "alpha-brass-nanoparticles"
    cs_ids = []
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": all_co_ids}},
        ravel=True,
    ).tolist()

    print(
        "Configuration set ",
        f"({name}):".rjust(22),
        f"{len(co_ids)}".rjust(7),
    )

    cs_id = client.insert_configuration_set(
        co_ids, description=f"Set of {name}", name=name
    )
    cs_ids.append(cs_id)

    # Gather copper-only set
    name = "Cu-only-alpha-brass-nanoparticles"
    cu_ids = client.get_data(
        "configurations",
        fields=["hash", "nelements"],
        query={"hash": {"$in": all_co_ids}, "nelements": {"$eq": 1}},
        ravel=True,
    )["hash"]
    print(
        "Configuration set ",
        f"({name}):".rjust(22),
        f"{len(cu_ids)}".rjust(7),
    )

    cs_id = client.insert_configuration_set(
        cu_ids,
        description="Set from alpha-brass nanoparticles dataset containing "
        "only copper",
        name=name,
    )

    cs_ids.append(cs_id)

    name = "CuZn-only-alpha-brass-nanoparticles"
    cuzn_ids = cu_ids = client.get_data(
        "configurations",
        fields=["hash", "nelements"],
        query={"hash": {"$in": all_co_ids}, "nelements": {"$eq": 2}},
        ravel=True,
    )["hash"]
    print(
        "Configuration set ",
        f"({name}):".rjust(22),
        f"{len(cu_ids)}".rjust(7),
    )
    cs_id = client.insert_configuration_set(
        cuzn_ids,
        description="Set from alpha-brass nanoparticles dataset containing "
        "copper and zinc (i.e., no copper-only molecules)",
        name=name,
    )
    cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name="alpha_brass_nanoparticles",
        authors=[
            "Jan Weinreich",
            "Anton Römer",
            "Martín Leandro Paleico",
            "Jörg Behler",
        ],
        links=[
            "http://doi.org/10.1021/acs.jpcc.0c00559",
            "https://doi.org/10.24435/materialscloud:94-aq",
        ],
        description="53,841 structures of alpha-brass (less than 40% Zinc)."
        " Includes atomic forces and total energy. Calculated using VASP at "
        "the DFT level of theory.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

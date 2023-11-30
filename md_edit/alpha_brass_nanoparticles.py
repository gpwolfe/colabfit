"""
author:gpwolfe

Properties:
potential energy
forces

Other properties added to metadata:
None

File notes
----------
"""

from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from collections import defaultdict
import numpy as np
from pathlib import Path
import sys

# DATASET_FP = Path(
#     "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/alpha_brass_nanoparticles"
# )
# local
DATASET_FP = Path().cwd().parent / "data/alpha_brass_nanoparticles/brass_DFT_data"
DS_NAME = "alpha_brass_nanoparticles"

PUBLICATION = "https://doi.org/10.1021/acs.jpcc.1c02314"
DATA_LINK = "https://doi.org/10.24435/materialscloud:94-aq"
OTHER_LINKS = ["http://doi.org/10.1021/acs.jpcc.0c00559"]
LINKS = [
    "https://doi.org/10.1021/acs.jpcc.1c02314",
    "http://doi.org/10.1021/acs.jpcc.0c00559",
    "https://doi.org/10.24435/materialscloud:94-aq",
]
AUTHORS = [
    "Jan Weinreich",
    "Anton Römer",
    "Martín Leandro Paleico",
    "Jörg Behler",
]
DS_DESC = (
    "53,841 structures of alpha-brass (less than 40% Zinc)."
    " Includes atomic forces and total energy. Calculated using VASP at "
    "the DFT level of theory."
)

metadata = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    # "k-point":{"value":"12 x 12 x 12"}, # for smaller systems. Larger use gamma-center
    "encut": {"value": "500 eV"},
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
    pds = [atomic_forces_pd, potential_energy_pd]
    for pd in pds:
        client.insert_property_definition(pd)
    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Cu", "Zn"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    css = (
        (
            "Cu-only-alpha-brass-nanoparticles",
            {"nelements": {"$eq": 1}},
            "Set from alpha-brass nanoparticles dataset containing only copper",
        ),
        (
            "CuZn-only-alpha-brass-nanoparticles",
            {"nelements": {"$eq": 2}},
            "Set from alpha-brass nanoparticles dataset containing copper and zinc "
            "(i.e., no copper-only molecules)",
        ),
    )

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
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

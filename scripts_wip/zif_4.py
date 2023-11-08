"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
energy file header:
Step Nr.   Time[fs]  Kin.[a.u.]  Temp[K]  Pot.[a.u.]  Cons Qty[a.u.]  UsedTime[s]
This file will not be used, as potential energy is also reported in the position file


cell file header:
Step   Time [fs]       Ax [Angstrom]       Ay [Angstrom]       Az [Angstrom]\
    Bx [Angstrom]       By [Angstrom]       Bz [Angstrom]       Cx [Angstrom]\
    Cy [Angstrom]       Cz [Angstrom]      Volume [Angstrom^3]

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import iread

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/zeolitic_imidazolate")
DATASET_NAME = "Amorphous_Zeolitic_Imidazolate_Frameworks_2023"

SOFTWARE = "CP2K"
METHODS = "DFT-PBE-D3"

PUBLICATION = "https://doi.org/10.26434/chemrxiv-2023-8003d"
DATA_LINK = "https://doi.org/10.5281/zenodo.10015594"
LINKS = [
    "https://doi.org/10.5281/zenodo.10015594",
    "https://doi.org/10.26434/chemrxiv-2023-8003d",
]
AUTHORS = [
    "Nicolas Castel",
    "Dune Andre",
    "Connor Edwards",
    "Jack D. Evans",
    "Francois-Xavier Coudert",
]
DATASET_DESC = (
    "This dataset contains four trajectories of ZIF-4 liquids calculated at different "
    "volumes and temperatures, and one trajectory of the ZIF-4 crystal at 300 "
    "K, generated by density functional theory (DFT) calculations. "
)
ELEMENTS = None
GLOB_STR = "*.*"

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
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "bar"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "enthalpy": {"field": "h", "units": "Ha"},
    "zpve": {"field": "zpve", "units": "Ha"},
}

DSS = [
    [
        f"{DATASET_NAME}_aluminum",
        {"names": {"$regex": "aluminum"}},
        f"Configurations of aluminum from {DATASET_NAME} dataset",
    ]
]


def read_cell_stress(cell_fp):
    with open(cell_fp, "r") as f:
        f.readline()
        for line in f.readlines():
            ce = line.strip().split()[2:]
            cell = [[ce[0], ce[1], ce[2]], [ce[4], ce[5], ce[6]], [ce[7], ce[8], ce[9]]]
            yield cell


def read_forces(force_fp):
    for c in iread(force_fp, index=":", format="extxyz"):
        yield c.positions


def reader(pos_fp):
    cell_fp = next(pos_fp.parent.glob("*.cell"))
    name = cell_fp.stem.lower().replace("-", "_")
    configs = iread(pos_fp, index=":", format="extxyz")
    cells = read_cell_stress(cell_fp)
    force_fp = next(pos_fp.parent.glob("*.frc"))
    stress_fp = next(pos_fp.parent.glob("*.stress"))
    forces = read_forces(force_fp)
    stresses = read_cell_stress(stress_fp)
    for i, config in enumerate(configs):
        config.cell = next(cells)
        config.info["forces"] = next(forces)
        config.info["stress"] = next(stresses)
        config.info["name"] = f"{name}_{i}"
        yield config


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=True,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
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
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

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
import json
from pathlib import Path
import sys

# from ase.io import read
from ase.io import Trajectory


# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/tmQM_wB97MV/all_data/tmQM_wB97MV/")
DATASET_NAME = "tmQM_wB97MV"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

SOFTWARE = "Q-Chem"
METHODS = "DFT-wB97M-V"

PUBLICATION = "https://doi.org/10.1021/acs.jcim.3c01226"
DATA_LINK = "https://github.com/ulissigroup/tmQM_wB97MV"
# OTHER_LINKS = []

AUTHORS = [
    "Aaron G. Garrison",
    "Javier Heras-Domingo",
    "John R. Kitchin",
    "Gabriel dos Passos Gomes",
    "Zachary W. Ulissi",
    "Samuel M. Blau",
]
DATASET_DESC = (
    "tmQM_wB97MV contains configurations from the tmQM dataset, "
    "with several structures from tmQM that were found to be missing hydrogens "
    "filtered out, and energies of all other structures recomputed at the "
    "wB97M-V/def2-SVPD level of DFT."
)
ELEMENTS = None
GLOB_STR = "tmQM_wB97MV.traj"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    # "basis-set": {"field": "basis_set"}
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "formation-energy": [
        {
            "energy": {"field": "target", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "reference-energy": {"field": "ref_energy", "units": "hartree"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "GPa"},
    #         "volume-normalized": {"value": True, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

CO_METADATA = {
    "total-charge": {"field": "q"},
    "spin": {"field": "spin"},
    "CSD-code": {"field": "CSD_code"},
}


def reader(fp):
    data = Trajectory(fp)
    for atoms in data:
        atoms.info["name"] = f"tmQM_wB97MV_{atoms.info['CSD_code']}_{atoms.info['SID']}"
        atoms.info["ref_energy"] = sum(
            [ref_energy[num] for num in atoms.arrays["numbers"]]
        )
        yield atoms


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

    with open("formation_energy.json") as f:
        formation_energy_pd = json.load(f)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(formation_energy_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
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


ref_energy = {
    1: -0.60,
    5: -24.85,
    6: -38.07,
    7: -54.70,
    8: -75.16,
    9: -99.75,
    14: -289.44,
    15: -341.23,
    16: -398.05,
    17: -460.07,
    21: -760.62,
    22: -849.37,
    23: -943.85,
    24: -1044.23,
    25: -1150.73,
    26: -1263.44,
    27: -1382.45,
    28: -1508.03,
    29: -1640.13,
    30: -1779.07,
    33: -2235.49,
    34: -2401.16,
    35: -2573.75,
    39: -38.31,
    40: -47.03,
    41: -56.93,
    42: -68.11,
    43: -80.69,
    44: -94.77,
    45: -110.41,
    46: -127.76,
    47: -146.75,
    48: -167.61,
    53: -297.74,
    57: -31.49,
    72: -48.02,
    73: -56.95,
    74: -67.01,
    75: -78.22,
    76: -90.53,
    77: -104.24,
    78: -119.22,
    79: -135.56,
    80: -153.30,
}

if __name__ == "__main__":
    main(sys.argv[1:])

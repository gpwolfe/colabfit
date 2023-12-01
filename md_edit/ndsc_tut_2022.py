"""
author:gpwolfe

Data as zipfile can be downloaded from:
https://github.com/ConnorSA/ndsc_tut/tree/master/example_data

File address:
https://github.com/ConnorSA/ndsc_tut/blob/master/example_data/hcp_Mg_geomopti_randshear_pm_0.01_product_symm_k0p012.extxyz

Unzip to script folder
unzip ndsc_tut-master.zip "*xyz" -d <project_dir>/scripts/ndsc_tut/

Change DATASET_FP to reflect path to file (not parent folder)
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>
Properties:
forces
virial

Other properties added to metadata:
total energy


File notes
----------
xyz file header:
Lattice
Properties=species:S:1:pos:R:3:forces:R:3
energy  <-- this appears to be total energy, from the publication
virial
config_type=hcp_Mg_geomopti_randshear_pm_0.01_product_symm_k0p012
pbc
"""

from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/ndsc_tut/"
    "ndsc_tut-master/example_data/"
    "hcp_Mg_geomopti_randshear_pm_0.01_product_symm_k0p012.extxyz"
)
DS_NAME = "NDSC_TUT_2022"
PUBLICATION = "https://doi.org/10.48550/arXiv.2207.11828"
DATA_LINK = "https://github.com/ConnorSA/ndsc_tut"
LINKS = [
    "https://github.com/ConnorSA/ndsc_tut",
    "https://doi.org/10.48550/arXiv.2207.11828",
]
AUTHORS = ["Connor Allen", "Albert P. Bartok"]
DS_DESC = (
    "500 configurations of Mg2 for MD prediction using a model "
    "fitted on Al, W, Mg and Si."
)


def reader(file_path):
    file_name = file_path.stem
    atom = ase.io.read(file_path)
    atom.info["name"] = file_name
    yield atom


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
        file_format="extxyz",
        name_field=None,
        elements=["Mg"],
        reader=reader,
        # glob_string='*xyz',
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": "CASTEP"},
        "method": {"value": "DFT"},
        "input": {
            "value": {"encut": {"value": 520, "units": "eV"}},
            "kspacing": {"value": 0.012, "units": "Ang^-1"},
        },
    }
    co_md = {"config_type": {"field": "config_type"}}
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
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=co_md,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

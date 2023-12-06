"""
author:

Data can be downloaded from:


Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
From Dylan's unfinished script
Dylan's original note:
Data being uploaded from Si_md.extxyz is half of a dataset obtained
from a paper called 'Sensitivity and Dimensionality...' by Onat, Ortner, andn Kermode
( https://aip.scitation.org/doi/10.1063/5.0016005 ).
Data downloaded from this is cited as data from 'Representations in neural network
based empirical potentials' ( https://aip.scitation.org/doi/10.1063/1.4990503 ) as
a colleague said that it is the same data. I am placing this note here as since the
data in the supplementary information is not in a format I know how to read so I
cannot confirm it. If a customer messages about this data, please see this and direct
them using this info.
"""

from argparse import ArgumentParser
from ase.io import read
from pathlib import Path
import sys

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/Si_Berk/")
DATASET_FP = Path().cwd().parent / "data/si_jcp_2017"
DS_NAME = "Si_JCP_2017"
DS_DESC = (
    "A dataset of 64-atom silicon configurations in four phases: cubic-diamond, "
    "(beta)-tin, R8, and liquid. MD simulations are run at 300, 600 and 900 K for "
    "solid phases; up to 2500 K for the L phase. All relaxations performed at zero "
    "pressure. Additional configurations prepared by random distortion of crystal "
    "structures. VASP was used with a PAW pseudopotential and PBE exchange "
    "correlation. k-point mesh was optimized for energy convergence of 0.5 meV/atom "
    "and stress convergence of 0.1 kbar. The plane wave energy cutoff was set to 300 "
    "eV. To reduce the correlation between data points MD, data were thinned by using "
    "one of every 100 consecutive structures from the MD simulations at 300 K and one "
    "of every 20 structures from higher temperature MD simulations."
)
AUTHORS = [
    "Ekin D. Cubuk",
    "Brad D. Malone",
    "Berk Onat",
    "Amos Waterland",
    "Efthimios Kaxiras",
]

PUBLICATION = "https://doi.org/10.1063/1.4990503"
DATA_LINK = "https://doi.org/10.1063/1.4990503"
LINKS = [
    "https://doi.org/10.1063/1.4990503",
]
GLOB = "Si_md.extxyz"

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {"field": "input"},
}
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_MD,
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
}
CO_MD = {
    "kinetic-energy": {"field": "kinetic_energy"},
    "temperature": {"field": "temperature"},
}


def reader(fp):
    name = fp.stem
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
        config.info["input"] = {
            "encut": {"value": 300, "units": "eV"},
        }
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

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        reader=reader,
        elements=["Si"],
        verbose=False,
        generator=False,
        glob_string=GLOB,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(free_energy_pd)

    # kinetic_energy_property_definition = {
    #     "property-id": "kinetic-energy",
    #     "property-name": "kinetic-energy",
    #     "property-title": "kinetic-energy",
    #     "property-description": "kinetic energy",
    #     "energy": {
    #         "type": "float",
    #         "has-unit": True,
    #         "extent": [],
    #         "required": True,
    #         "description": "kinetic energy",
    #     },
    # }

    # client.insert_property_definition(kinetic_energy_property_definition)

    ids = list(
        client.insert_data(
            configurations,
            co_md_map=CO_MD,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

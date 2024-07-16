"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
The configurations in the AIMD folder have no properties associated.
Additionally, 14 files of the ~3500 are missing the final totals of
energy and forces. ASE's FHI-aims out file parser fails to parse these
files. These are skipped.

"""
from argparse import ArgumentParser
from pathlib import Path
import sys
from tqdm import tqdm

from ase.io.aims import read_aims_output
from ase.calculators.singlepoint import SinglePointCalculator


# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/reactive_hydrogen_acs_2023/")
DATASET_NAME = "reactive_hydrogen_ACS_2023"
LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://pubs.acs.org/doi/full/10.1021/acs.jpcc.3c06648"
DATA_LINK = "https://dx.doi.org/10.17172/NOMAD/2023.05.03-2"
# OTHER_LINKS = []

AUTHORS = [
    "Wojciech G. Stark",
    "Julia Westermayr",
    "Oscar A. Douglas-Gallardo",
    "James Gardner",
    "Scott Habershon",
    "Reinhard J. Maurer",
]
DATASET_DESC = (
    "This dataset contains structures of Cu, including Cu(111), Cu(100), Cu(110), "
    "and Cu(211). Slab settings are as follows: 3 x 3, 6-layered slabs for Cu(111), "
    "(100), and (110) surfaces; 1 x 3, 6-layered slabs for Cu(211) surface. "
    "Includes some structures representing interation of H2 with one of the Cu "
    "surfaces and some structures of Cu sampled at different temperatures."
)
ELEMENTS = ["Cu", "H"]
GLOB_STR = "manifest.json"

PI_METADATA = {
    "software": {"value": "FHI-aims"},
    "method": {"value": "DFT-SRP"},
    "input": {
        "value": {
            "k-grid": "12x12x1",
            "basis-set": "tight",
            "total-energy-convergence": {"value": 10e-6, "units": "eV"},
            "eigenvalue-energy-convergence": {"value": 10e-3, "units": "eV"},
            "charge-density-convergence": {"value": 10e-5, "units": "e/angstom^3"},
            "force-convergence": {"value": 10e-4, "units": "eV/angstrom"},
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
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
}


CSS = [
    [
        f"{DATASET_NAME}_Cu",
        {"nelements": 1},
        f"Configurations containing just Cu from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_HCu",
        {"nelements": 2},
        f"Configurations containing H and Cu from {DATASET_NAME} dataset",
    ],
]


def reader(fp):
    try:
        config = read_aims_output(fp)
        if not isinstance(config.calc, SinglePointCalculator):
            return None
        else:
            config.info["energy"] = config.get_potential_energy()
            config.info["forces"] = config.get_forces()
            config.info["name"] = f"{'__'.join(fp.parts[-6:-1])}"
            return config
    except IndexError:
        pass
        # with open("errors_hydrogen.txt", "a") as f:
        #     f.write(f"{fp}\t\t{e}\n")


def reader_wrapper(target_fp):
    fps = target_fp.parent.rglob("aims*.out")  # target is for load_data function
    for fp in tqdm(fps):
        config = reader(fp)
        if config is not None:
            # print([config])
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader_wrapper,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=False,
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
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

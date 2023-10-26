"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
energy
stress
free energy


Other properties added to metadata
----------------------------------
momenta

File notes
----------

"""

from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    cauchy_stress_pd,
    potential_energy_pd,
    free_energy_pd,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/LML-retrain/")
DATASET_FP = Path("").cwd().parent / "data/lml_retrain"

PUBLICATION = "https://doi.org/10.1016/j.actamat.2023.118734"
DATA_LINK = "https://github.com/marseille-matmol/LML-retrain"
LINKS = [
    "https://github.com/marseille-matmol/LML-retrain",
    "https://doi.org/10.1016/j.actamat.2023.118734",
]
AUTHORS = ["Berk Onat", "Christoph Ortner", "James R. Kermode"]
DATASET_DESC = (
    "The W_LML-retrain dataset contains DFT calculations used in testing a "
    "linear-in-descriptor machine learning potential that accounts for "
    "dislocation-defect interactions in tungsten. "
    "Density functional simulations were performed using VASP. The PBE "
    "generalised gradient approximation was used to describe effects of electron "
    "exchange and correlation together with a projector augmented wave (PAW) basis "
    "set with a cut-off energy of 550 eV. Occupancies were smeared with a "
    "Methfessel-Paxton scheme of order one with a 0.1 eV smearing width. "
    "The Brillouin zone was sampled with a Monkhorst-Pack k-point grid for "
    "the 2D cluster simulations periodic along the dislocation line and a single "
    "k-point was used for the calculations with 3D spherical QM regions. The "
    "values of these parameters were chosen after a series of convergence tests "
    "on forces with a tolerance of a few meV/Å."
)
ELEMENTS = None
GLOB_STR = "*.xyz"

# Assign additional relevant property instance metadata, such as basis set used
PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "temperature": {"field": "Temperature"},
    "energy-cutoff": {"550 eV"},
}
PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "kbar"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


# Define any configuration-specific metadata here.
CO_METADATA = {
    "momenta": {"field": "momenta"},
}


def reader(fp: Path):
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{fp.stem}_{i}"

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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(free_energy_pd)

    dss = (
        (
            "W_LML-retrain_bulk_MD_test",
            "bulk_MD_test",
            "Test set from W_LML-retrain dataset, containing bulk tungsten "
            "calculations. ",
        ),
        (
            "W_LML-retrain_bulk_MD_train",
            "bulk_MD_train",
            "Training set from W_LML-retrain dataset, containing bulk tungsten "
            "calculations. ",
        ),
        (
            "W_LML-retrain_vacancy_MD_test",
            "vac_MD_test",
            "Test set from W_LML-retrain dataset, containing calculations of "
            "tungsten with vacancies. ",
        ),
        (
            "W_LML-retrain_vacancy_MD_train",
            "vac_MD_train",
            "Training set from W_LML-retrain dataset, containing calculations of "
            "tungsten with vacancies. ",
        ),
    )
    for name, reg, desc in dss:
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
                property_map=PROPERTY_MAP,
                co_md_map=CO_METADATA,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=LINKS,
            description=desc + DATASET_DESC,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author: Gregory Wolfe

Properties
----------
forces
energy
free energy

Other properties added to metadata
----------------------------------
density
process

File notes
----------
Should be safe to run from ingest pod

Three datasets are created:
a test split, a train split (from the 216-atom configurations)
a set including all 216 and 512-atom configurations, divided
into configuration sets

keys:
 Lattice
 Properties=species:S:1:pos:R:3:forces:R:3
 energy
 free_energy
 process
 density
 pbc

・216atom_amorphous: Contains six xyz files generated from the trajectory
 of the melt-quench simulation.

・216atom_crystal: Contains a single xyz file with data of diamond structures
 containing 216 atoms at densities of 2.4, 2.6, 2.8, 3.0, 3.2, 3.4, and 3.5 g/cm3.

・512atom_amorphous: Contains a single xyz file with data of 512-atom systems.

・dataset_train_test_split: The training and test data used in the manuscript,
 constructed from splitting the entire 216atom_amorphous dataset.
 test:
 train:


"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/aCdataset-zenodo")
DATASET_NAME = "aC_JCP_2023"

SOFTWARE = "VASP"
METHODS = "LDA"

PUBLICATION = "https://doi.org/10.1063/5.0159349"
DATA_LINK = "https://doi.org/10.5281/zenodo.7905585"
LINKS = ["https://doi.org/10.5281/zenodo.7905585", "https://doi.org/10.1063/5.0159349"]
AUTHORS = ["Emi Minamitani", "Ippei Obayashi", "Koji Shimizu", "Satoshi Watanabe"]
DATASET_DESC = (
    "The amorphous carbon dataset was generated using ab initio calculations with VASP "
    "software. We utilized the LDA exchange-correlation functional and the PAW "
    "potential for carbon. Melt-quench simulations were performed to create "
    "amorphous and liquid-state structures. A simple cubic lattice of 216 "
    "carbon atoms was chosen as the initial state. Simulations were conducted "
    "at densities of 1.5, 1.7, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.2, 3.4, and 3.5 "
    "g/cm3 to produce a variety of structures. The NVT ensemble was employed for "
    "all melt-quench simulations, and the density was adjusted by modifying the "
    "size of the simulation cell. A time step of 1 fs was used for the simulations. "
    "For all densities, only the Γ points were sampled in the k-space. To increase "
    "structural diversity, six independent simulations were performed."
    "In the melt-quench simulations, the temperature was raised from 300 K to 9000 K "
    "over 2 ps to melt carbon. Equilibrium molecular dynamics (MD) was conducted "
    "at 9000 K for 3 ps to create a liquid state, followed by a decrease in "
    "temperature to 5000 K over 2 ps, with the system equilibrating at "
    "that temperature for 2 ps. Finally, the temperature was lowered from 5000 "
    "K to 300 K over 2 ps to generate an amorphous structure."
    "During the melt-quench simulation, 30 snapshots were taken from the equilibrium "
    "MD trajectory at 9000 K, 100 from the cooling process between 9000 and 5000 K, "
    "25 from the equilibrium MD trajectory at 5000 K, and 100 from the cooling "
    "process between 5000 and 300 K. This yielded a total of 16,830 data points."
    "Data for diamond structures containing 216 atoms at densities of 2.4, 2.6, 2.8, "
    "3.0, 3.2, 3.4, and 3.5 g/cm3 were also prepared. Further data on the diamond "
    "structure were obtained from 80 snapshots taken from the 2 ps equilibrium MD "
    "trajectory at 300 K, resulting in 560 data points."
    "To validate predictions for larger structures, we generated data for 512-atom "
    "systems using the same procedure as for the 216-atom systems. A single "
    "simulation was conducted for each density. The number of data points was 2,805 "
    "for amorphous and liquid states"
)
ELEMENTS = None
GLOB_STR = "*.xyz"

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
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
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
}

CO_METADATA = {
    "process": {"field": "process"},
    "density": {"field": "density"},
}

CSS = [
    [
        f"{DATASET_NAME}_216_atom_amorphous",
        {"names": {"$regex": "216atom_amorphous"}},
        f"Configurations from {DATASET_NAME} melt-quench simulations with 216 atoms",
    ],
    [
        f"{DATASET_NAME}_216_atom_crystal",
        {"names": {"$regex": "216atom_crystal"}},
        f"Configurations from {DATASET_NAME} diamond crystal simulations",
    ],
    [
        f"{DATASET_NAME}_512_atom_amorphous",
        {"names": {"$regex": "512atom_amorphous"}},
        f"Configurations from {DATASET_NAME} melt-quench simulations with 512 atoms",
    ],
]

DSS = [
    (
        f"{DATASET_NAME}_test",
        DATASET_FP / "dataset_train_test_split",
        "test.xyz",
        f"Test split from the 216-atom amorphous portion of the {DATASET_NAME} "
        "dataset. " + DATASET_DESC,
    ),
    (
        f"{DATASET_NAME}_train",
        DATASET_FP / "dataset_train_test_split",
        "train.xyz",
        f"Test split from the 216-atom amorphous portion of the {DATASET_NAME} "
        "dataset. " + DATASET_DESC,
    ),
]


def reader(fp: Path):
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{fp.parts[-2]}_{fp.stem}_{i}"
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)

    for i, (ds_name, fp, ds_glob, desc) in enumerate(DSS[:2]):
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=fp,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=ds_glob,
            generator=False,
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

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=LINKS,
            description=desc,
            verbose=True,
        )
    ds_id = generate_ds_id()
    configurations = []

    for fp in [
        DATASET_FP / "216atom_amorphous",
        DATASET_FP / "216atom_crystal",
        DATASET_FP / "512atom_amorphous",
    ]:
        configurations += load_data(
            file_path=fp,
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

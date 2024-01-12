"""
author: gpwolfe

Properties
----------
Forces
Potential energy

File notes
----------
tested locally, should run w/out problem on Kubernetes

"""
from argparse import ArgumentParser
from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/md_22")  # HSRN
DATASET_FP = Path().cwd().parent / "data/md22"  # local

SOFTWARE = "FHI-aims"
METHODS = "DFT-PBE+MBE"

PUBLICATION = "https://doi.org/10.1126/sciadv.adf0873"
DATA_LINK = "http://sgdml.org/"
LINKS = ["https://doi.org/10.1126/sciadv.adf0873", "http://sgdml.org/"]
AUTHORS = [
    "Stefan Chmiela",
    "Valentin Vassilev-Galindo",
    "Oliver T. Unke",
    "Adil Kabylda",
    "Huziel E. Sauceda",
    "Alexandre Tkatchenko",
    "Klaus-Robert Müller",
]
DESC = (
    "MD22 represents a collection of datasets in a benchmark that can be "
    "considered an updated version of the MD17 benchmark datasets, including more "
    "challenges with respect to system size, flexibility and degree of non-locality. "
    "The datasets in MD22 include MD trajectories of the protein Ac-Ala3-NHMe; the "
    "lipid DHA (docosahexaenoic acid); the carbohydrate stachyose; nucleic acids AT-AT "
    "and AT-AT-CG-CG; and the buckyball catcher and double-walled nanotube "
    "supramolecules. Each of these is included here in a separate dataset, as "
    "represented on sgdml.org. Calculations were performed using FHI-aims and i-Pi "
    "software at the DFT-PBE+MBD level of theory. Trajectories were sampled at "
    "temperatures between 400-500 K at 1 fs resolution."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "basis-set": {"field": "basis-set"},
    "thermostat": {"field": "thermostat"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "Energy", "units": "kcal/mol"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "kcal/mol/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
}
basis_dict = {
    "md22_Ac-Ala3-NHMe": ("tight", "Global Langevin, friction coefficient=2 fs"),
    "md22_AT-AT-CG-CG": ("tight", "Global Langevin, friction coefficient=2 fs"),
    "md22_AT-AT": ("tight", "Global Langevin, friction coefficient=2 fs"),
    "md22_buckyball-catcher": ("light", "Nosé-Hoover, effective mass=1700/cm"),
    "md22_DHA": ("tight", "Nosé-Hoover, effective mass=1700/cm"),
    "md22_double-walled_nanotube": ("light", "Nosé-Hoover, effective mass=1700/cm"),
    "md22_stachyose": ("tight", "Nosé-Hoover, effective mass=1700/cm"),
}


def get_basis(stem):
    return basis_dict[stem]


def reader(fp):
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{fp.stem}_{i}"
        config.info["basis-set"], config.info["thermostat"] = get_basis(fp.stem)
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    dss = (
        (
            "MD22_Ac_Ala3_NHMe",
            "md22_Ac-Ala3-NHMe.xyz",
            "Dataset containing MD trajectories of the 42-atom tetrapeptide "
            f"Ac-Ala3-NHMe from the MD22 benchmark set. {DESC}",
        ),
        (
            "MD22_AT_AT_CG_CG",
            "md22_AT-AT-CG-CG.xyz",
            "Dataset containing MD trajectories of AT-AT-CG-CG DNA base pairs from the "
            f"MD22 benchmark set. {DESC}",
        ),
        (
            "MD22_AT_AT",
            "md22_AT-AT.xyz",
            "Dataset containing MD trajectories of AT-AT DNA base pairs from the MD22 "
            "benchmark set. {DESC}",
        ),
        (
            "MD22_buckyball_catcher",
            "md22_buckyball-catcher.xyz",
            "Dataset containing MD trajectories of the buckyball-catcher supramolecule "
            f"from the MD22 benchmark set. {DESC}",
        ),
        (
            "MD22_DHA",
            "md22_DHA.xyz",
            "Dataset containing MD trajectories of DHA (docosahexaenoic acid) from the "
            f"MD22 benchmark set. {DESC}",
        ),
        (
            "MD22_double_walled_nanotube",
            "md22_double-walled_nanotube.xyz",
            "Dataset containing MD trajectories of the double-walled nanotube "
            f"supramolecule from the MD22 benchmark set. {DESC}",
        ),
        (
            "MD22_stachyose",
            "md22_stachyose.xyz",
            "Dataset containing MD trajectories of the tetrasaccharide stachyose from "
            f"the MD22 benchmark set. {DESC}",
        ),
    )
    for name, glob, desc in dss:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=glob,
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

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=desc,
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

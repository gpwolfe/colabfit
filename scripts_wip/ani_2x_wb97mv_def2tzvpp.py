"""
author: Gregory Wolfe

Properties
----------
potential energy

Other properties added to metadata
----------------------------------
dipole, original scf energy without the energy correction subtracted

File notes
----------
"energy" is the result of the wB97M_def2-TZVPP.scf-energies minus the
VV10.energy-corrections, as given in the source file.
"""

from argparse import ArgumentParser
from functools import partial
import h5py
from pathlib import Path
from tqdm import tqdm
import sys


from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    # atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/ani2x/final_h5/")
DATASET_NAME = "ANI-2x-wB97MV-def2TZVPP"

LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "https://doi.org/10.1021/acs.jctc.0c00121"
DATA_LINK = "https://doi.org/10.5281/zenodo.10108942"
# OTHER_LINKS = []

AUTHORS = [
    "Kate Huddleston",
    "Roman Zubatyuk",
    "Justin Smith",
    "Adrian Roitberg",
    "Olexandr Isayev",
    "Ignacio Pickering",
    "Christian Devereux",
    "Kipton Barros",
]
DATASET_DESC = (
    "ANI-2x-wB97MV-def2TZVPP is a portion of the ANI-2x dataset, which includes "
    "DFT-calculated energies for structures from 2 to 63 atoms in size containing "
    "H, C, N, O, S, F, and Cl. This portion of ANI-2x was calculated at the WB97MV "
    "level of theory using the def2TZVPP basis set. Configuration sets are divided "
    "by number of atoms per structure."
)
ELEMENTS = None
GLOB_STR = "ANI-2x-wB97MV-def2TZVPP.h5"
PI_METADATA = {
    "software": {"value": "ORCA 4.2.1"},
    "method": {"value": "DFT-wB97MV"},
    "basis_set": {"value": "def2-TZVPP"},
    "input": {
        "value": {
            "step-1:": r"""! quick-dft slowconv loosescf
%scf maxiter 256 end""",
            "step-2": '''! wB97m-d3bj def2-tzvpp def2/j rijcosx engrad \
tightscf SCFConvForced soscf grid4 finalgrid6 gridx7
%elprop dipole true quadrupole true end
%output PrintLevel mini Print[P DFTD GRAD] 1 end
%scf maxiter 256 end
! MORead
%moinp "PATH TO .gbw FILE FROM STEP 1"''',
            "step-3": '''! wb97m-v def2-tzvpp def2/j rijcosx tightscf \
ScfConvForced grid4 finalgrid6 gridx7 vdwgrid4
! MORead
%moinp "PATH TO .gbw FILE FROM STEP 2"''',
        }
    },
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "reference-energy": {"field": "en_correction", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "dipole": {"field": "dipole", "units": "electron angstrom"},
    "wB97M-def2-TZVPP-scf-energy": {"field": "scf_energy", "units": "hartree"},
}

CSS = [
    [
        f"{DATASET_NAME}_num_atoms_{natoms}",
        {"names": {"$regex": f"natoms_{natoms:03d}__"}},
        f"Configurations with {natoms} atoms from {DATASET_NAME} dataset",
    ]
    # for natoms in range(2, 64)
    for natoms in range(2, 4)
]


def ani_reader(fp, num_atoms):
    with h5py.File(fp) as h5:
        properties = h5[str(num_atoms)]
        coordinates = properties["coordinates"]
        species = properties["species"]
        energies = properties["energies"]
        en_correction = properties["VV10.energy-corrections"]
        scf_energies = properties["wB97M_def2-TZVPP.scf-energies"]
        dipoles = properties["dipoles"]
        for i, coord in enumerate(coordinates):
            config = AtomicConfiguration(
                positions=coord,
                numbers=species[i],
            )
            config.info["energy"] = energies[i]
            config.info["en_correction"] = en_correction[i]
            config.info["scf_energy"] = scf_energies[i]
            config.info["dipole"] = dipoles[i]
            config.info["name"] = f"ANI-2x-wB97MV-def2TZVPP__natoms_{num_atoms}__ix_{i}"
            yield config


def read_wrapper(fp, dbname, uri, nprocs, ds_id):
    client = MongoDatabase(dbname, uri=uri, nprocs=nprocs)
    with h5py.File(fp) as h5:
        for num_atoms in tqdm(h5.keys(), desc="num_atoms"):
            partial_read = partial(ani_reader, fp)
            ids = []
            configurations = load_data(
                file_path=DATASET_FP,
                file_format="folder",
                name_field="name",
                elements=ELEMENTS,
                reader=partial_read,
                glob_string=GLOB_STR,
                generator=False,
            )
            ids.append(
                client.insert_data(
                    configurations=configurations,
                    ds_id=ds_id,
                    co_md_map=CO_METADATA,
                    property_map=PROPERTY_MAP,
                    generator=False,
                    verbose=False,
                )
            )
    return ids


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

    client.insert_property_definition(potential_energy_pd)
    ds_id = generate_ds_id()
    partial_read_wrapper = partial(
        read_wrapper, client.database_name, client.uri, client.nprocs, args.ds_id
    )

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=ani_reader,
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

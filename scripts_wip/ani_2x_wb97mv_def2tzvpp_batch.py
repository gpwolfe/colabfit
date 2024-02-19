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

Keys for the h5 file are 3-digit strings from "002" to "063", with some missing numbers,
according to the number of atoms in the configuration.
"""

from argparse import ArgumentParser
from functools import partial
import functools
import logging
import pymongo
import h5py
from pathlib import Path
import time
from tqdm import tqdm
import sys


from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    # atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/ani2x/final_h5")
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


def ani_reader(num_atoms, fp):
    with h5py.File(fp) as h5:
        # print(num_atoms)
        properties = h5[str(num_atoms)]
        coordinates = properties["coordinates"]
        species = properties["species"]
        energies = properties["energies"]
        en_correction = properties["VV10.energy-corrections"]
        scf_energies = properties["wB97M_def2-TZVPP.scf-energies"]
        dipoles = properties["dipoles"]
        configs = []
        while True:
            for i, coord in enumerate(coordinates):
                config = AtomicConfiguration(
                    positions=coord,
                    numbers=species[i],
                )
                config.info["energy"] = energies[i]
                config.info["en_correction"] = en_correction[i]
                config.info["scf_energy"] = scf_energies[i]
                config.info["dipole"] = dipoles[i]
                config.info["name"] = (
                    f"ANI-2x-wB97MV-def2TZVPP__natoms_{num_atoms}__ix_{i}"
                )
                configs.append(config)
                if len(configs) == 50000:
                    for config in configs:
                        yield config
                    configs = []
            if len(configs) > 0:
                for config in configs:
                    yield config
                break


def read_wrapper(dbname, uri, nprocs, ds_id, n_atoms):
    wrap_time = time.time()
    client = MongoDatabase(dbname, uri=uri, nprocs=nprocs)
    ids = []
    fp = next(DATASET_FP.rglob(GLOB_STR))
    today = time.strftime("%Y-%m-%d")
    with h5py.File(fp) as h5:
        print(h5.keys())
        for num_atoms in tqdm(n_atoms, desc=f"N atoms: {n_atoms[0]}-{n_atoms[-1]}"):
            if num_atoms in h5.keys():
                partial_read = partial(ani_reader, num_atoms)

                insert_time = time.time()

                configurations = load_data(
                    file_path=DATASET_FP,
                    file_format="folder",
                    name_field="name",
                    elements=ELEMENTS,
                    reader=partial_read,
                    glob_string=GLOB_STR,
                    generator=True,
                )
                ids_batch = list(
                    client.insert_data(
                        configurations=configurations,
                        ds_id=ds_id,
                        co_md_map=CO_METADATA,
                        property_map=PROPERTY_MAP,
                        generator=False,
                        verbose=False,
                    )
                )
                ids.extend(ids_batch)
                new_insert_time = time.time()
                print(f"Time to insert: {new_insert_time - insert_time}")
                insert_time = new_insert_time
                co_ids, do_ids = list(zip(*ids_batch))
                co_id_file = Path(
                    f"{ds_id}_co_ids_{today}/{ds_id}_"
                    "config_ids_batch_natoms_{num_atoms}.txt"
                )
                co_id_file.parent.mkdir(parents=True, exist_ok=True)
                do_id_file = Path(
                    f"{ds_id}_do_ids_{today}/{ds_id}_do_ids_batch_natoms_{num_atoms}"
                    ".txt"
                )
                do_id_file.parent.mkdir(parents=True, exist_ok=True)
                with open(co_id_file, "a") as f:
                    f.writelines([f"{id}\n" for id in co_ids])
                with open(do_id_file, "a") as f:
                    f.writelines([f"{id}\n" for id in do_ids])
    print(f"Time to read: {time.time() - wrap_time}")
    return ids


MAX_AUTO_RECONNECT_ATTEMPTS = 100


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                if wait_t > 1800:
                    wait_t = 1800  # cap at 1/2 hour
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


@auto_reconnect
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
    parser.add_argument(
        "-s", "--ds_id", type=str, help="Dataset ID to use for dataset", default=None
    )
    parser.add_argument(
        "-a", "--start", type=int, help="Start index for dataset", default=0
    )
    parser.add_argument(
        "-o", "--end", type=int, help="End index for dataset", default=64
    )

    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    ds_id = args.ds_id
    if ds_id is None:
        ds_id = generate_ds_id()
    n_atoms = [f"{x:03d}" for x in range(args.start, args.end + 1)]
    print(n_atoms)
    client.insert_property_definition(potential_energy_pd)

    ids = read_wrapper(
        dbname=client.database_name,
        uri=client.uri,
        nprocs=client.nprocs,
        ds_id=ds_id,
        n_atoms=n_atoms,
    )
    print("Num. Ids: ", len(ids))
    # all_co_ids, all_do_ids = list(zip(*ids))

    # cs_ids = []
    # for i, (name, query, desc) in enumerate(CSS):
    #     cs_id = client.query_and_insert_configuration_set(
    #         co_hashes=all_co_ids,
    #         ds_id=ds_id,
    #         name=name,
    #         description=desc,
    #         query=query,
    #     )

    #     cs_ids.append(cs_id)

    # client.insert_dataset(
    #     do_hashes=all_do_ids,
    #     ds_id=ds_id,
    #     name=DATASET_NAME,
    #     authors=AUTHORS,
    #     links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
    #     description=DATASET_DESC,
    #     verbose=False,
    #     cs_ids=cs_ids,  # remove line if no configuration sets to insert
    #     data_license=LICENSE,
    # )


DB_KEYS = [
    "002",
    "003",
    "004",
    "005",
    "006",
    "007",
    "008",
    "009",
    "010",
    "011",
    "012",
    "013",
    "014",
    "015",
    "016",
    "017",
    "018",
    "019",
    "020",
    "021",
    "022",
    "023",
    "024",
    "025",
    "026",
    "027",
    "028",
    "029",
    "030",
    "031",
    "032",
    "033",
    "034",
    "035",
    "036",
    "037",
    "038",
    "039",
    "040",
    "041",
    "042",
    "043",
    "044",
    "045",
    "046",
    "047",
    "048",
    "049",
    "050",
    "051",
    "052",
    "053",
    "054",
    "055",
    "056",
    "057",
    "058",
    "062",
    "063",
]
CSS = [
    [
        f"{DATASET_NAME}_num_atoms_{natoms}",
        {"names": {"$regex": f"natoms_{natoms}__"}},
        f"Configurations with {natoms} atoms from {DATASET_NAME} dataset",
    ]
    # for natoms in range(2, 64)
    for natoms in DB_KEYS
]
if __name__ == "__main__":
    main(sys.argv[1:])

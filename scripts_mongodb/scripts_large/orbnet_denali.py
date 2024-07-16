"""
File notes
----------
Entos Qcore appears to be the software used

denali_labels file line format:
sample_id,subset (hash in file name),mol_id, test_set,test_set_plus,prelim_1, \
    training_set_plus,charge,dft_energy,xtb1_energy
2059108,f876445191fc24469cf8f64fc9a07e9695a0aecea8b9f097dfb0dc8582205d9e,conformers,CHEMBL364217_conformers,False,False,False,True,0,-1588.0915069900238,-56.10287377905384

"""
import functools

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd

from argparse import ArgumentParser
import pandas as pd
from ase.io import read
import logging
from pathlib import Path
import pymongo
import sys
from time import time
from tqdm import tqdm

DATASET_FP = Path("/vast/gw2338/orbnet/")
# DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/OrbNet_Denali")
# DATASET_FP = Path("data/orbnet")  # remove
DS_NAME = "OrbNet_Denali"

PUBLICATION = "https://doi.org/10.1063/5.0061990"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.14883867.v2"
LINKS = [
    "https://doi.org/10.1063/5.0061990",
    "https://doi.org/10.6084/m9.figshare.14883867.v2",
]
AUTHORS = [
    "Anders S. Christensen",
    "Sai Krishna Sirumalla",
    "Zhuoran Qiao",
    "Michael B. OConnor",
    "Daniel G. A. Smith",
    "Feizhi Ding",
    "Peter J. Bygrave",
    "Animashree Anandkumar",
    "Matthew Welborn",
    "Frederick R. Manby",
    "Thomas F. Miller III",
]
DS_DESC = (
    "All DFT single-point calculations for the OrbNet Denali "
    "training set were carried out in Entos Qcore version 0.8.17 "
    "at the ωB97X-D3/def2-TZVP level of theory using in-core "
    "density fitting with the neese=4 DFT integration grid."
)


def reader_OrbNet(fp):
    df = pd.read_csv(fp)
    df = df.iloc[:, 1:]  # remove row limit
    structures = []
    for row in tqdm(df.itertuples()):
        f = DATASET_FP / "xyz_files" / row.mol_id / f"{row.sample_id}.xyz"
        structure = read(f)
        structure.info["energy"] = row.dft_energy
        structure.info["xtb1_energy"] = row.xtb1_energy
        structure.info["charge"] = row.charge
        structures.append(structure)

    return structures


def tform(c):
    c.info["per-atom"] = False


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=None,
        default_name="orbnet-denali",
        reader=reader_OrbNet,
        glob_string="denali_labels.csv",
        verbose=False,
        generator=False,
    )
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "ENTOS QCORE 0.8.17"},
                    "method": {"value": "DFT-ωB97X-D3"},
                    "basis-set": {"value": "def2-TZVP"},
                    "input": {"value": {"neese": 4}},
                },
            }
        ],
    }
    co_md = {
        "xtb1-energy": {"field": "xtb1_energy"},
        "charge": {"field": "charge"},
    }

    client.insert_property_definition(potential_energy_pd)
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            co_md_map=co_md,
            property_map=property_map,
            ds_id=ds_id,
            generator=False,
            transform=tform,
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
    begin = time()
    main(sys.argv[1:])
    print(f"Total time (sec): {time() - begin}")

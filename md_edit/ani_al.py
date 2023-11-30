"""
author:gpwolfe

Data can be downloaded from:

Download link:
https://github.com/atomistic-ml/ani-al/archive/refs/heads/master.zip

Extract to project folder
tar xzf ani-al-master/data/Al-data.tgz -C $project_dir/scripts/ani_al/data
mv ani-al-master/model/model-Al-75/testset/* $project_dir/scripts/ani_al/data

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------
fermi (for training data)

File notes
----------
train set keys returned using the repository's anidataloader class:
'path', 'Jnames', 'cell', 'coordinates', 'energy', 'fermi', 'force', 'species'
of these, Jnames and path appear to reference information local to the data
source file system, therefore not included

test set keys:
'path', 'cell', 'charges', 'coordinates', 'dipoles', 'energies', 'forces',
'species'
values for charges and dipoles all appear to be null values

From supplementary information:
The ANI-Al predicted total system energy ˆE = ∑
i ˆEi is a sum over local contributions centered
on each atom i.

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys

import h5py
import os

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/ani_al")
# DATASET_FP = Path().cwd().parent / "data/Al-data"  # remove
DATASET = "ANI-Al_NC2021"

SOFTWARE = "Quantum ESPRESSO"
METHODS = "DFT-PBE"
PUBLICATION = "https://doi.org/10.1038/s41467-021-21376-0"
DATA_LINK = "https://github.com/atomistic-ml/ani-al"
LINKS = [
    "https://github.com/atomistic-ml/ani-al",
    "https://doi.org/10.1038/s41467-021-21376-0",
]
AUTHORS = [
    "Justin S. Smith",
    "Benjamin Nebgen",
    "Nithin Mathew",
    "Jie Chen",
    "Nicholas Lubbers",
    "Leonid Burakovsky",
    "Sergei Tretiak",
    "Hai Ah Nam",
    "Timothy Germann",
    "Saryu Fensin",
    "Kipton Barros",
]
ELEMENTS = ["Al"]


def reader(filepath):
    dl = anidataloader(filepath)
    configs = []
    for i, data in enumerate(dl):
        config = AtomicConfiguration(
            symbols=data["species"],
            positions=data["coordinates"][0],
            cell=data["cell"][0],
        )
        config.info["energy"] = data.get("energy", data.get("energies"))[0]
        config.info["fermi"] = data.get("fermi", [None])[0]
        config.info["forces"] = data.get("force", data.get("forces"))[0]
        config.info["name"] = f"{filepath.stem}_{i}"
        configs.append(config)

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

    metadata = {"software": {"value": SOFTWARE}, "method": {"value": METHODS}}
    co_md_map = {"fermi": {"field": "fermi"}}
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "Quantum ESPRESSO"},
                    "method": {"value": "DFT-PBE"},
                    "kpoint": {"value": "3x3x3 grid"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }

    for glob_ds in [("data-*.h5", "train"), ("testset*.h5", "test")]:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=glob_ds[0],
            generator=False,
        )
        ds_id = generate_ds_id()

        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                co_md_map=co_md_map,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))
        # cs_regexes = [
        #     [
        #         f"{DATASET}-train",
        #         "data*",
        #         f"Training set from {DATASET} dataset",
        #     ],
        #     [
        #         f"{DATASET}-test",
        #         "testset*",
        #         f"Test set from {DATASET} dataset",
        #     ],
        # ]

        # cs_ids = []

        # for i, (name, regex, desc) in enumerate(cs_regexes):
        #     co_ids = client.get_data(
        #         "configurations",
        #         fields="hash",
        #         query={
        #             "hash": {"$in": all_co_ids},
        #             "names": {"$regex": regex},
        #         },
        #         ravel=True,
        #     ).tolist()

        #     print(
        #         f"Configuration set {i}",
        #         f"({name}):".rjust(22),
        #         f"{len(co_ids)}".rjust(7),
        #     )
        #     if len(co_ids) > 0:
        #         cs_id = client.insert_configuration_set(co_ids, description=desc,
        #                                                 name=name)

        #         cs_ids.append(cs_id)
        #     else:
        #         pass

        client.insert_dataset(
            # cs_ids=cs_ids,
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=f"{DATASET}-{glob_ds[1]}",
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=(
                f"Approximately 2800 configurations from a {glob_ds[1]} dataset–one of "
                "a pair of train/test datasets of aluminum in crystal and melt "
                "phases, used for training and testing an ANI neural network model."
            ),
            verbose=True,
        )


# Below lines taken from pyanitools.py. Sourced with dataset from:
# https://github.com/atomistic-ml/ani-al
# Written by Roman Zubatyuk and Justin S. Smith


class anidataloader(object):

    """Contructor"""

    def __init__(self, store_file):
        if not os.path.exists(store_file):
            exit("Error: file not found - " + store_file)
        self.store = h5py.File(store_file)

    """ Group recursive iterator (iterate through all groups in all
    branches and return datasets in dicts) """

    def h5py_dataset_iterator(self, g, prefix=""):
        for key in g.keys():
            item = g[key]
            path = "{}/{}".format(prefix, key)
            keys = [i for i in item.keys()]
            if isinstance(item[keys[0]], h5py.Dataset):  # test for dataset
                data = {"path": path}
                for k in keys:
                    if not isinstance(item[k], h5py.Group):
                        dataset = np.array(item[k][()])

                        if type(dataset) is np.ndarray:
                            if dataset.size != 0:
                                if type(dataset[0]) is np.bytes_:
                                    dataset = [a.decode("ascii") for a in dataset]

                        data.update({k: dataset})

                yield data
            else:  # test for group (go down)
                yield from self.h5py_dataset_iterator(item, path)

    """ Default class iterator (iterate through all data) """

    def __iter__(self):
        for data in self.h5py_dataset_iterator(self.store):
            yield data

    """ Returns a list of all groups in the file """

    def get_group_list(self):
        return [g for g in self.store.values()]

    """ Allows interation through the data in a given group """

    def iter_group(self, g):
        for data in self.h5py_dataset_iterator(g):
            yield data

    """ Returns the requested dataset """

    def get_data(self, path, prefix=""):
        item = self.store[path]
        path = "{}/{}".format(prefix, path)
        keys = [i for i in item.keys()]
        data = {"path": path}
        # print(path)
        for k in keys:
            if not isinstance(item[k], h5py.Group):
                dataset = np.array(item[k][()])

                if type(dataset) is np.ndarray:
                    if dataset.size != 0:
                        if type(dataset[0]) is np.bytes_:
                            dataset = [a.decode("ascii") for a in dataset]

                data.update({k: dataset})
        return data

    """ Returns the number of groups """

    def group_size(self):
        return len(self.get_group_list())

    def size(self):
        count = 0
        for g in self.store.values():
            count = count + len(g.items())
        return count

    """ Close the HDF5 file """

    def cleanup(self):
        self.store.close()


if __name__ == "__main__":
    main(sys.argv[1:])

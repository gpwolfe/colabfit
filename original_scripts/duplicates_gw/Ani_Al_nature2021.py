#!/usr/bin/env python
# coding: utf-8
"""
File notes:
gpwolfe also has a script to upload this dataset without
pyanitools dependency
"""
from argparse import ArgumentParser
import h5py
import os
from pathlib import Path
import sys

from ase import Atoms
import numpy as np
from colabfit.tools.database import MongoDatabase, load_data

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets/ani-al/"
    "ani-al-master/data/"
)
DATASET = "ANI-Al_NC2021"

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

DS_DESC = (
    "Approximately 2800 configurations from a training dataset and 2800 from a "
    "test dataset of aluminum in crystal and melt phases, used for training and "
    "testing the ANI neural network model. Built using an active learning scheme "
    "and used to drive non-equilibrium molecular dynamics simulations with "
    "time-varying applied temperatures."
)


def reader(path):
    adl = anidataloader(path)
    images = []
    for data in adl:
        atoms = Atoms(symbols=data["species"], positions=data["coordinates"][0])
        atoms.info["per-atom"] = False
        atoms.info["species"] = data["species"]
        atoms.info["energy"] = data["energy"][0]
        images.append(atoms)

    return images


def tform(c):
    c.info["per-atom"] = False


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="species",
        elements=["Al"],
        default_name="ANI-Al",
        verbose=True,
        reader=reader,
        glob_string="*.h5",
    )

    # client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
    # client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
    # client.insert_property_definition("/home/ubuntu/notebooks/cauchy-stress.json")

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
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
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

"""
author:

Data can be downloaded from:


Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

methods, software (where stated) and basis sets as follows:
"hf_dz.energy": meth("HF",  "cc-pVDZ"),
"hf_qz.energy": meth("HF", "cc-pVQZ"),
"hf_tz.energy": meth("HF", "cc-pVTZ"),
# "mp2_dz.corr_energy": meth("MP2", "cc-pVDZ"),
# "mp2_qz.corr_energy": meth("MP2", "cc-pVQZ"),
# "mp2_tz.corr_energy": meth("MP2", "cc-pVTZ"),
# "npno_ccsd(t)_dz.corr_energy": meth("NPNO-CCSD(T)", "cc-pVDZ"),
# "npno_ccsd(t)_tz.corr_energy": meth("NPNO-CCSD(T)", "cc-pVTZ"),
# "tpno_ccsd(t)_dz.corr_energy": meth("TPNO-CCSD(T)", "cc-pVDZ"),
# "wb97x_dz.cm5_charges": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_dz.dipole": meth("wB97x", "Gaussian 09","6-31G*"),
"wb97x_dz.energy": meth("wB97x", "Gaussian 09","6-31G*"),
"wb97x_dz.forces": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_dz.hirshfeld_charges": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_dz.quadrupole": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_tz.dipole": meth("wB97x","ORCA","def2-TZVPP"),
"wb97x_tz.energy": meth("wB97x", "ORCA","def2-TZVPP"),
"wb97x_tz.forces": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_charges": meth("wB97x","ORCA", "def2-TZVPP"),
# "wb97x_tz.mbis_dipoles": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_octupoles": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_quadrupoles": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_volumes": meth("wB97x", "ORCA","def2-TZVPP"),
"ccsd(t)_cbs.energy": meth("CCSD(T)*", "ORCA","CBS"),
# "wb97x_tz.quadrupole": meth("wB97x", "ORCA","def2-TZVPP"),

"""
from argparse import ArgumentParser
from collections import namedtuple
import h5py
from pathlib import Path
import sys

import numpy as np

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/ani1x")
DATASET_NAME = "ANI-1x"
LINKS = [
    "https://doi.org/10.1038/s41597-020-0473-z",
    "https://doi.org/10.1063/1.5023802",
    "https://doi.org/10.1038/s41467-019-10827-4",
    "https://doi.org/10.1126/sciadv.aav6490",
    "https://doi.org/10.6084/m9.figshare.c.4712477.v1",
    "https://github.com/aiqm/ANI1x_datasets",
]
AUTHORS = [
    "Justin S. Smith",
    "Roman Zubatyuk",
    "Benjamin Nebgen",
    "Nicholas Lubbers",
    "Kipton Barros",
    "Adrian E. Roitberg",
    "Olexandr Isayev",
    "Sergei Tretiak",
]
DATASET_DESC = ""
ELEMENTS = None
GLOB_STR = "*.h5"
meth = namedtuple(typename="meth", field_names=["method", "software", "basis"])
method = {
    "hf_dz.energy": meth("HF", "ORCA", "cc-pVDZ"),
    "hf_qz.energy": meth("HF", "ORCA", "cc-pVQZ"),
    "hf_tz.energy": meth("HF", "ORCA", "cc-pVTZ"),
    "wb97x_dz.energy": meth("wB97x", "Gaussian 09", "6-31G*"),
    "wb97x_dz.forces": meth("wB97x", "Gaussian 09", "6-31G*"),
    "wb97x_tz.energy": meth("wB97x", "ORCA", "def2-TZVPP"),
    "wb97x_tz.forces": meth("wB97x", "ORCA", "def2-TZVPP"),
    "ccsd(t)_cbs.energy": meth("CCSD(T)*", "ORCA", "CBS"),
}
PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["energy"].software},
                "method": {"value": method["energy"].method},
                "basis-set": {"value": method["energy"].basis - set},
            },
        },
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["energy"].software},
                "method": {"value": method["energy"].method},
                "basis-set": {"value": method["energy"].basis - set},
            },
        },
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["energy"].software},
                "method": {"value": method["energy"].method},
                "basis-set": {"value": method["energy"].basis - set},
            },
        },
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["energy"].software},
                "method": {"value": method["energy"].method},
                "basis-set": {"value": method["energy"].basis - set},
            },
        },
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["energy"].software},
                "method": {"value": method["energy"].method},
                "basis-set": {"value": method["energy"].basis - set},
            },
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "eV"},
    #         "volume-normalized": {"value": True, "units": None},
    #         "_metadata": metadata,
    #     }
    # ],
}

CO_METADATA = {}


def read_h5(h5filename):
    """
    Sourced from https://github.com/aiqm/ANI1x_datasets/tree/master
    Iterate over buckets of data in ANI HDF5 file.
    Yields dicts with atomic numbers (shape [Na,]) coordinated (shape [Nc, Na, 3])
    and other available properties specified by `keys` list, w/o NaN values.
    """

    with h5py.File(h5filename, "r") as f:
        keys = set(list(f.values())[0].keys())
        keys.discard("atomic_numbers")
        keys.discard("coordinates")
        for grp in f.values():
            Nc = grp["coordinates"].shape[0]
            mask = np.ones(Nc, dtype=bool)
            data = dict((k, grp[k][()]) for k in keys)
            for k in keys:
                v = data[k].reshape(Nc, -1)
                mask = mask & ~np.isnan(v).any(axis=1)
            if not np.sum(mask):
                continue
            d = dict((k, data[k][mask]) for k in keys)
            d["atomic_numbers"] = grp["atomic_numbers"][()]
            d["coordinates"] = grp["coordinates"][()][mask]
            yield d


def reader(filepath: Path):
    configs = []
    for data in read_h5(filepath):
        atomic_nums = data.pop("atomic_numbers")
        for i, coord in enumerate(data.pop("coordinates")):
            config = AtomicConfiguration(positions=coord, numbers=atomic_nums)
            config.info = {key: val[i] for key, val in data.items()}
            config.info["name"] = f"{filepath.stem}_{i}"
            configs.append(config)
            if i > 2000:
                break
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # If no obvious divisions between configurations exist (i.e., different methods or
    # materials), remove the following lines through 'cs_ids.append(...)' and from
    # 'insert_dataset(...) function remove 'cs_ids=cs_ids' argument.

    cs_regexes = [
        [
            f"{DATASET_NAME}_aluminum",
            "aluminum",
            f"Configurations of aluminum from {DATASET_NAME} dataset",
        ]
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

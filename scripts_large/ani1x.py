"""
author:



Properties
----------
potential energy
atomic forces

Other properties added to metadata
----------------------------------
dipole
quadrupole
octupole
volume

File notes
----------
Run successfully from Greene using NodePort connection; ingested to colabfit-web
16.10.2023

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
# "wb97x_tz.quadrupole": meth("wB97x", "ORCA","def2-TZVPP")


Units for props are described here:
https://www.nature.com/articles/s41597-020-0473-z/tables/2
"""
from argparse import ArgumentParser
from collections import namedtuple
import h5py
from pathlib import Path
import subprocess
import sys

import numpy as np

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/ani1x/")  # HSRN
DATASET_FP = Path("data/ani1x")  # local and Greene
DATASET_NAME = "ANI-1x"

PUBLICATION = "https://doi.org/10.1038/s41597-020-0473-z"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.c.4712477.v1"
OTHER_LINKS = [
    "https://doi.org/10.1063/1.5023802",
    "https://doi.org/10.1038/s41467-019-10827-4",
    "https://doi.org/10.1126/sciadv.aav6490",
    "https://github.com/aiqm/ANI1x_datasets",
]
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
DATASET_DESC = (
    "ANI-1x contains DFT calculations for approximately 5 million molecular "
    "conformations. From an initial training set, an active learning method was used "
    "to iteratively add conformations where insufficient diversity was detected. "
    "Additional conformations were sampled from existing databases of molecules, such "
    "as GDB-11 and ChEMBL. On each of these configurations, one of molecular dynamics "
    "sampling, normal mode sampling, dimer sampling, or torsion sampling was performed."
)
ELEMENTS = None
GLOB_STR = "ani1x-release.h5"
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
            "energy": {"field": "hf_dz.energy", "units": "Ha"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["hf_dz.energy"].software},
                "method": {"value": method["hf_dz.energy"].method},
                "basis-set": {"value": method["hf_dz.energy"].basis},
            },
        },
        {
            "energy": {"field": "hf_qz.energy", "units": "Ha"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["hf_qz.energy"].software},
                "method": {"value": method["hf_qz.energy"].method},
                "basis-set": {"value": method["hf_qz.energy"].basis},
            },
        },
        {
            "energy": {"field": "hf_tz.energy", "units": "Ha"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["hf_tz.energy"].software},
                "method": {"value": method["hf_tz.energy"].method},
                "basis-set": {"value": method["hf_tz.energy"].basis},
            },
        },
        {
            "energy": {"field": "wb97x_dz.energy", "units": "Ha"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["wb97x_dz.energy"].software},
                "method": {"value": method["wb97x_dz.energy"].method},
                "basis-set": {"value": method["wb97x_dz.energy"].basis},
            },
        },
        {
            "energy": {"field": "wb97x_tz.energy", "units": "Ha"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["wb97x_tz.energy"].software},
                "method": {"value": method["wb97x_tz.energy"].method},
                "basis-set": {"value": method["wb97x_tz.energy"].basis},
            },
        },
        {
            "energy": {"field": "ccsd(t)_cbs.energy", "units": "Ha"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": method["ccsd(t)_cbs.energy"].software},
                "method": {"value": method["ccsd(t)_cbs.energy"].method},
                "basis-set": {"value": method["ccsd(t)_cbs.energy"].basis},
            },
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "wb97x_dz.forces", "units": "Ha/A"},
            "_metadata": {
                "software": {"value": method["wb97x_dz.forces"].software},
                "method": {"value": method["wb97x_dz.forces"].method},
                "basis-set": {"value": method["wb97x_dz.forces"].basis},
            },
        },
        {
            "forces": {"field": "wb97x_tz.forces", "units": "Ha/A"},
            "_metadata": {
                "software": {"value": method["wb97x_tz.forces"].software},
                "method": {"value": method["wb97x_tz.forces"].method},
                "basis-set": {"value": method["wb97x_tz.forces"].basis},
            },
        },
    ],
}


def read_h5(h5filename):
    """
    Inspired by https://github.com/aiqm/ANI1x_datasets/tree/master
    Iterate over buckets of data in ANI HDF5 file.
    Yields dicts with atomic numbers (shape [Na,]) coordinated (shape [Nc, Na, 3])
    and other available properties specified by `keys` list, w/o NaN values.
    """

    with h5py.File(h5filename, "r") as f:
        keys = set(list(f.values())[0].keys())
        keys.discard("atomic_numbers")
        keys.discard("coordinates")
        for grp in f.values():
            data = {k: list(grp[k]) for k in keys}
            a_nums = list(grp["atomic_numbers"])
            for i, coords in enumerate(grp["coordinates"]):
                row = {k: data[k][i] for k in keys if (~np.isnan(data[k][i]).any())}
                row["coords"] = coords
                row["a_nums"] = a_nums
                yield row


def reader(filepath: Path):
    configs = []
    for i, row in enumerate(read_h5(filepath)):
        config = AtomicConfiguration(
            positions=row.pop("coords"), numbers=row.pop("a_nums")
        )
        config.info = {key: val for key, val in row.items()}
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
    args = parser.parse_args(argv)

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
    # For forwarding from Greene
    subprocess.run("kubectl port-forward svc/mongo 5000:27017 &", shell=True)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:5000"
    )
    # For running locally
    # client = MongoDatabase(
    #     args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    # )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            co_md_map=CO_METADATA,
            ds_id=ds_id,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # cs_regexes = [
    #     [
    #         f"{DATASET_NAME}_aluminum",
    #         "aluminum",
    #         f"Configurations of aluminum from {DATASET_NAME} dataset",
    #     ]
    # ]

    # cs_ids = []

    # for i, (name, regex, desc) in enumerate(cs_regexes):
    #     cs_id = client.query_and_insert_configuration_set(
    #         co_hashes=all_co_ids,
    #         ds_id=ds_id,
    #         name=name,
    #         description=desc,
    #         query={"names": {"$regex": regex}},
    #     )

    #     cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


CO_METADATA = {  # "hf_dz.energy": meth("HF",  "cc-pVDZ"),
    # "hf_qz.energy": meth("HF", "cc-pVQZ"),
    # "hf_tz.energy": meth("HF", "cc-pVTZ"),
    "mp2_dz.corr_energy": {"field": "mp2_dz.corr_energy"},
    "mp2_qz.corr_energy": {"field": "mp2_qz.corr_energy"},
    "mp2_tz.corr_energy": {"field": "mp2_tz.corr_energy"},
    "npno_ccsd(t)_dz.corr_energy": {"field": "npno_ccsd(t)_dz.corr_energy"},
    "npno_ccsd(t)_tz.corr_energy": {"field": "npno_ccsd(t)_tz.corr_energy"},
    "tpno_ccsd(t)_dz.corr_energy": {"field": "tpno_ccsd(t)_dz.corr_energy"},
    "wb97x_dz.cm5_charges": {"field": "wb97x_dz.cm5_charges"},
    "wb97x_dz.dipole": {"field": "wb97x_dz.dipole"},
    # "wb97x_dz.energy": meth("wB97x"}, "Gaussian 09"},"6-31G*"),
    # "wb97x_dz.forces": meth("wB97x"}, "Gaussian 09"},"6-31G*"),
    "wb97x_dz.hirshfeld_charges": {"field": "wb97x_dz.hirshfeld_charges"},
    "wb97x_dz.quadrupole": {"field": "wb97x_dz.quadrupole"},
    "wb97x_tz.dipole": {"field": "wb97x_tz.dipole"},
    # "wb97x_tz.energy": meth("wB97x"}, "ORCA"},"def2-TZVPP"),
    # "wb97x_tz.forces": meth("wB97x"}, "ORCA"},"def2-TZVPP"),
    "wb97x_tz.mbis_charges": {"field": "wb97x_tz.mbis_charges"},
    "wb97x_tz.mbis_dipoles": {"field": "wb97x_tz.mbis_dipoles"},
    "wb97x_tz.mbis_octupoles": {"field": "wb97x_tz.mbis_octupoles"},
    "wb97x_tz.mbis_quadrupoles": {"field": "wb97x_tz.mbis_quadrupoles"},
    "wb97x_tz.mbis_volumes": {"field": "wb97x_tz.mbis_volumes"},
    # "ccsd(t)_cbs.energy": meth("CCSD(T)*"}, "ORCA"},"CBS"),
    "wb97x_tz.quadrupole": {"field": "wb97x_tz.quadrupole"},
}

if __name__ == "__main__":
    main(sys.argv[1:])

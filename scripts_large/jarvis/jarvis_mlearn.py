"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
fund = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
optb88vdw_total_energy (dft_3d)
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

Keys
[
'atoms',
'energy'
'forces',
'jid',
'stresses'
]
"""

from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

from colabfit.tools.property_definitions import (
    potential_energy_pd,
    cauchy_stress_pd,
    atomic_forces_pd,
)


DATASET_FP = Path("jarvis_json/")
GLOB = "mlearn.json"
DS_NAME = "JARVIS_mlearn"
DS_DESC = (
    "The JARVIS_mlearn dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the Organic Materials Database (OMDB): a dataset of 12,500 "
    "crystal materials for the purpose of training models for the prediction of "
    "properties for complex and lattice-periodic organic crystals with large numbers "
    "of atoms per unit cell. Dataset covers 69 space groups, 65 elements; averages 82 "
    "atoms per unit cell. "
    "This dataset also includes classical force-field inspired descriptors (CFID) for "
    "each configuration. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LINKS = [
    "https://figshare.com/ndownloader/files/40424156",
    "https://jarvis.nist.gov/",
    "https://github.com/materialsvirtuallab/mlearn",
    "https://doi.org/10.1021/acs.jpca.9b08723",
]
AUTHORS = [
    "Yunxing Zuo",
    "Chi Chen",
    "Xiangguo Li",
    "Zhi Deng",
    "Yiming Chen",
    "Jörg Behler",
    "Gábor Csányi",
    "Alexander V. Shapeev",
    "Aidan P. Thompson",
    "Mitchell A. Wood",
    "Shyue Ping Ong",
]
ELEMENTS = None

PI_MD = {
    "software": {"value": "VASP 5.4.1"},
    "method": {"value": "DFT-PBE"},
    "k-point-mesh": {"field": "kpoint"},
    "cutoff": {"value": "520 eV"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_MD,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stresses", "units": "kbar"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


# with open("formation_energy.json", "r") as f:
#     formation_energy_pd = json.load(f)
# with open("band_gap.json", "r") as f:
#     band_gap_pd = json.load(f)


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
        data = data
    configs = []
    for i, row in enumerate(data):
        atoms = row.pop("atoms")
        elements = atoms["elements"]
        if atoms["cartesian"] is True:
            config = AtomicConfiguration(
                positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        else:
            config = AtomicConfiguration(
                scaled_positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        if atoms["elements"][0] == "Li":
            kpoint = "3 x 3 x 3"
        else:
            kpoint = "4 x 4 x 4"
        config.info["name"] = f"mlearn_{elements[0]}_{i}"
        config.info["kpoint"] = kpoint
        for key, val in row.items():
            if type(val) == str and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif type(val) == list and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif type(val) == dict and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (type(val) == float or type(val) == int) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
            else:
                pass
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
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    # client = MongoDatabase(  # for remote testing: remove
    #     args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:5000"
    # )

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB,
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

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
    css = [
        (
            "JARVIS-mlearn-Ni",
            "mlearn_Ni.*",
            "Ni configurations from JARVIS-mlearn dataset",
        ),
        (
            "JARVIS-mlearn-Ge",
            "mlearn_Ge.*",
            "Ge configurations from JARVIS-mlearn dataset",
        ),
        (
            "JARVIS-mlearn-Li",
            "mlearn_Li.*",
            "Li configurations from JARVIS-mlearn dataset",
        ),
        (
            "JARVIS-mlearn-Mo",
            "mlearn_Mo.*",
            "Mo configurations from JARVIS-mlearn dataset",
        ),
        (
            "JARVIS-mlearn-Si",
            "mlearn_Si.*",
            "Si configurations from JARVIS-mlearn dataset",
        ),
        (
            "JARVIS-mlearn-Cu",
            "mlearn_Cu.*",
            "Cu configurations from JARVIS-mlearn dataset",
        ),
    ]
    for name, reg, desc in css:
        client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            query={"names": {"$regex": reg}},
            name=name,
            description=desc,
            ds_id=ds_id,
        )

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


CO_KEYS = [
    # 'atoms',
    # 'energy',
    # 'forces',
    "jid",
    # 'stresses'
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

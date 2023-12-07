"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
potential energy
atomic forces

File notes
----------
"""


from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine"
)
# comment out below, local path
DATASET_FP = Path().cwd().parent / "data/benzene_succinic_acid_glycine"
DS_NAME = "benzene_succinic_acid_glycine_TS_PNAS_2022"
DS_DESC_PBE = (
    "DFT reference energies and forces were calculated using "
    "Quantum Espresso v6.3. The calculations were performed with "
    "the semi-local PBE xc functional, Tkatchenko-Scheffler dispersion correction, "
    "optimised norm-conserving Vanderbilt pseudopotentials, a "
    "Monkhorst-Pack k-point grid with a maximum spacing of 0.06 x 2π "
    "A^-1, and a plane-wave energy cut-off of 100 Ry for the wavefunction."
)
DS_DESC_MBD = (
    "Hybrid-functional DFT energies and forces were calculated using FHI-aims. "
    "The calculations were performed with the hybrid PBE0 functional and the MBD "
    "dispersion correCtion (PBE0-MBD), using a Monkhorst-Pack k-point grid with a "
    'maximum spacing of 0.06 x 2π A^-1, and the standard FHI-AIMS "intermediate" '
    "basis sets."
)
AUTHORS = ["Venkat Kapil", "Edgar A. Engel"]
PUBLICATION = "https://doi.org/10.1073/pnas.2111769119"
DATA_LINK = "https://doi.org/10.24435/materialscloud:vp-jf"
OTHER_LINKS = ["https://github.com/venkatkapil24/data_molecular_fluctuations"]
LINKS = [
    "https://doi.org/10.1073/pnas.2111769119",
    "https://doi.org/10.24435/materialscloud:vp-jf",
    "https://github.com/venkatkapil24/data_molecular_fluctuations",
]

PI_MD_PBE = {
    "software": {"value": "Quantum ESPRESSO v6.3"},
    "method": {"value": "DFT-PBE-TS"},
    "input": {"field": "input"},
}
PI_MD_MBD = {
    "software": {"value": "FHI-aims"},
    "method": {"value": "DFT-PBE0-MBD"},
    "input": {"field": "input"},
}

property_map_pbe = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD_PBE,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_MD_PBE,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/Ang^3"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_MD_PBE,
        }
    ],
}
property_map_mbd = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD_MBD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_MD_MBD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/Ang^3"},
            "_metadata": PI_MD_MBD,
        }
    ],
}
dss = (
    # PBE datasets
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_benzene_test",
        "benzene_test_QE_PBE_TS.xyz",
        'Benzene test PBE-TS dataset from "Semi-local and hybrid functional '
        "DFT data for thermalised snapshots of polymorphs of benzene, succinic "
        'acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_benzene_train",
        "benzene_train_FPS_QE_PBE_TS.xyz",
        'Benzene training PBE-TS dataset from "Semi-local and hybrid functional '
        "DFT data for thermalised snapshots of polymorphs of benzene, succinic "
        'acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_benzene_validation",
        "benzene_val_QE_PBE_TS.xyz",
        'Benzene validation PBE-TS dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_glycine_test",
        "glycine_test_QE_PBE_TS.xyz",
        'Glycine test PBE-TS dataset from "Semi-local and hybrid functional '
        "DFT data for thermalised snapshots of polymorphs of benzene, "
        'succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_glycine_train",
        "glycine_train_FPS_QE_PBE_TS.xyz",
        'Glycine training PBE-TS dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_glycine_validation",
        "glycine_val_QE_PBE_TS.xyz",
        'Glycine validation PBE-TS dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of benzene, "
        'succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_succinic_acid_test",
        "succinic_acid_test_QE_PBE_TS.xyz",
        'Succinic acid test PBE-TS dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of benzene, "
        'succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_succinic_acid_train",
        "succinic_acid_train_FPS_QE_PBE_TS.xyz",
        'Succinic acid training PBE-TS dataset from "Semi-local and '
        "hybrid functional DFT data for thermalised snapshots of polymorphs "
        'of benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE_TS_succinic_acid_validation",
        "succinic_acid_val_QE_PBE_TS.xyz",
        'Succinic acid validation PBE-TS dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of benzene, "
        'succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    # MBD datasets
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_benzene_test",
        "benzene_test_AIMS_PBE0_MBD.xyz",
        'Benzene test PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_benzene_train",
        "benzene_train_FPS_AIMS_PBE0_MBD.xyz",
        'Benzene training PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_benzene_validation",
        "benzene_val_AIMS_PBE0_MBD.xyz",
        'Benzene validation PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_glycine_test",
        "glycine_test_AIMS_PBE0_MBD.xyz",
        'Glycine test PBE0-MBD dataset from "Semi-local and hybrid functional '
        "DFT data for thermalised snapshots of polymorphs of benzene, succinic "
        'acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_glycine_train",
        "glycine_train_FPS_AIMS_PBE0_MBD.xyz",
        'Glycine training PBE0-MBD dataset from "Semi-local and hybrid functional '
        "DFT data for thermalised snapshots of polymorphs of benzene, succinic "
        'acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_glycine_validation",
        "glycine_val_AIMS_PBE0_MBD.xyz",
        'Glycine validation PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_succinic_acid_test",
        "succinic_acid_test_AIMS_PBE0_MBD.xyz",
        'Succinic acid test PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of "
        'benzene, succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_succinic_acid_train",
        "succinic_acid_train_FPS_AIMS_PBE0_MBD.xyz",
        'Succinic acid training PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of benzene, "
        'succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
    (
        "DFT_polymorphs_PNAS_2022_PBE0_MBD_succinic_acid_validation",
        "succinic_acid_val_AIMS_PBE0_MBD.xyz",
        'Succinic acid validation PBE0-MBD dataset from "Semi-local and hybrid '
        "functional DFT data for thermalised snapshots of polymorphs of benzene, "
        'succinic acid, and glycine". ' + DS_DESC_PBE,
    ),
)


def reader(fp):
    name = fp.stem
    configs = read(fp, index=":", format="extxyz")
    for config in configs:
        config.info["name"] = name
        kpoints = "x".join(config.info["kpts"].astype(str))
        config.info["input"] = {"encut": "100 Ry", "kpoints": kpoints}
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

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

    for i, (name, reg, desc) in enumerate(dss):
        ds_id = generate_ds_id()
        if "PBE" in name:
            property_map = property_map_pbe
        else:
            property_map = property_map_mbd

        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=None,
            verbose=False,
            reader=reader,
            generator=False,
            glob_string=reg,
        )

        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=property_map,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_pr_ids = list(zip(*ids))
        client.insert_dataset(
            do_hashes=all_pr_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
            description=desc,
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

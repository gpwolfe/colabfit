#!/usr/bin/env python
# coding: utf-8
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

DATASET_FP = Path("/new_raw_datasets_2.0/a-AlOx/")
# comment out below, local path
DATASET_FP = Path().cwd().parent / "data/benzene_succinic_acid_glycine"
DS_NAME = "benzene_succinic_acid_glycine_TS_PNAS_2022"
DS_DESC_PBE = (
    "DFT reference energies and forces were calculated using "
    "Quantum Espresso v6.3. The calculations were performed with "
    "the PBE xc functional, Tkatchenko-Scheffler dispersion correction, "
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
LINKS = [
    "https://doi.org/10.1073/pnas.2111769119",
    "https://doi.org/10.24435/materialscloud:vp-jf",
    "https://github.com/venkatkapil24/data_molecular_fluctuations",
]
GLOB = "*.xyzdat"

PI_MD_PBE = {
    "software": {"value": "Quantum ESPRESSO v6.3"},
    "method": {"value": "DFT-PBE-TS"},
    "ecut": {"value": "100 Ry"},
    "kpoints": {"value": "Monkhorst-Pack grid, max spacing: 0.06 x 2π A^-1"},
}
PI_MD_MBD = {
    "software": {"value": "FHI-aims"},
    "method": {"value": "DFT-PBE0-MBD"},
    "ecut": {"value": "100 Ry"},
    "kpoints": {"value": "Monkhorst-Pack grid, max spacing: 0.06 x 2π A^-1"},
}
PI_MD = {
    "software": {"field": "software"},
    "method": {"field": "method"},
    "ecut": {"field": "ecut"},
    "kpoints": {"field": "kpoints"},
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/Ang^3"},
            "_metadata": PI_MD,
        }
    ],
}


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
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/benzene_test_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H"],
        default_name="benzene_test_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/benzene_train_FPS_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H"],
        default_name="benzene_train_FPS_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/benzene_val_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H"],
        default_name="benzene_val_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/glycine_test_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H", "N", "O"],
        default_name="glycine_test_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/glycine_train_FPS_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H", "N", "O"],
        default_name="glycine_train_FPS_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/glycine_val_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H", "N", "O"],
        default_name="glycine_val_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/succinic_acid_test_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H", "O"],
        default_name="succinic_acid_test_QE_PBE_TS",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/succinic_acid_train_FPS_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H", "O"],
        default_name="succinic_acid_train_FPS_QE_PBE_TS",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/succinic_acid_val_QE_PBE_TS.xyz",
        file_format="xyz",
        name_field=None,
        elements=["C", "H", "O"],
        default_name="succinic_acid_val_QE_PBE_TS",
        verbose=True,
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    dss = (
        "benzene_test_QE_PBE_TS", 'Benzene test dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine". ',
        "benzene_train_FPS_QE_PBE_TS", 'Benzene training dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "benzene_val_QE_PBE_TS", 'Benzene validation dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "glycine_test_QE_PBE_TS", 'Glycine test dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "glycine_train_FPS_QE_PBE_TS", 'Glycine training dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "glycine_val_QE_PBE_TS", 'Glycine validation dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "succinic_acid_test_QE_PBE_TS", 'Succinic acid test dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "succinic_acid_train_FPS_QE_PBE_TS", 'Succinic acid training dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
        "succinic_acid_val_QE_PBE_TS", 'Succinic acid validation dataset from "Semi-local and hybrid functional DFT data for thermalised snapshots of polymorphs of benzene, succinic acid, and glycine".',
    )

    cs_names = [
        "benzene test",
        "benzene train",
        "benzene validation",
        "glycine test",
        "glycine train",
        "glycine validation",
        "succinic test",
        "succinic train",
        "succinic validation",
    ]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7)
        )

        cs_id = client.insert_configuration_set(
            co_ids, description=desc, name=cs_names[i]
        )

        cs_ids.append(cs_id)

    ds_id = client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

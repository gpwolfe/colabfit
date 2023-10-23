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
import pandas as pd
import tqdm as tqdm

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

DS_NAME = "Co_dimer_JPCA_2022"
DS_PATH = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/"
)
XYZ_PATH = DS_PATH / "structures_xyz"

LINKS = [
    "https://doi.org/10.1021/acs.jpca.1c08950",
    "https://doi.org/10.24435/materialscloud:pe-zv",
]
AUTHORS = [
    "Sijin Ren",
    "Eric Fonseca",
    "William Perry",
    "Hai-Ping Cheng",
    "Xiao-Guang Zhang",
    "Richard Hennig",
]
DS_DESC = (
    "A data set of 1081 Co(II) dimer molecules with the Co atoms in the high-spin "
    "state of S = 3/2. All molecules contain the same atomic core region, "
    "consisting of the tetrahedral and octahedral Co centers and the "
    "three PO2R2 bridging ligands. The ligand exchange provides a broad range of "
    "exchange energies, ΔEJ, from +50 to -200 meV, with 80% of the "
    "ligands yielding a small ΔEJ < 10 meV."
)


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "a.u."},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                # 'software': {'value':'VASP'},
                "method": {"value": "DFT/PBE"},
            },
        }
    ],
    #    'atomic-forces': [{
    #        'forces':   {'field': 'forces',  'units': 'eV/Ang'},
    #            '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }],
    #    'cauchy-stress': [{
    #        'stress':   {'field': 'virial',  'units': 'GPa'},
    #                '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #
    #    }],
}


def reader(fp):
    df = pd.read_csv(fp, index_col=0)
    # df2=pd.read_csv('/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/Co_dimer_data.csv',index_col=0)
    # df=pd.concat([df1, df2])
    # print(df)

    structures_FM = []
    structures_AFM = []
    # atoms=read(p,index=',')
    for row in tqdm(df.index):
        file_FM = str(XYZ_PATH / str(df.loc[row, "xyz_filename_FM"]))
        structure_FM = read(file_FM)
        # structures_FM.append(structure_FM)
        # print(structure_FM)
        structure_FM.info["energy"] = df.loc[row, "E-FM(a.u.)"].item()
        structures_FM.append(structure_FM)
        # print(type(structure_FM.info['energy_FM']))
        file_AFM = str(XYZ_PATH / str(df.loc[row, "xyz_filename_AFM"]))
        structure_AFM = read(file_AFM)
        structure_AFM.info["energy"] = df.loc[row, "E-AFM(a.u.)"].item()
        structures_AFM.append(structure_AFM)
        # print(structures_AFM)

    return structures_FM + structures_AFM


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

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

    dss = (
        (
            "Co_dimer_JPCA_2022_train",
            "Co_dimer_data.csv",
            "Training data only from the Co_dimer_JPCA_2022 dataset. " + DS_DESC,
        ),
        (
            "Co_dimer_JPCA_2022",
            "Co_dimer_data_all.csv",
            DS_DESC,
        ),
    )
    for name, glob, desc in dss:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DS_PATH,
            file_format="folder",
            name_field="name",
            elements=None,
            reader=reader,
            glob_string=glob,
            verbose=True,
            generator=False,
        )

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

    client.insert_dataset(
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

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
import pandas as pd
from ase import Atoms
from ase.io import read
from ase.io.vasp import read_vasp
from ase.db import connect
from tqdm import tqdm
import numpy as np

DS_NAME = "CoDimer_JPCA_2022"
DS_PATH = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/"
)

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
    "a data set of 1081 Co(II) dimer molecules with the Co atoms in the high-spin "
    "state of S = 3/2. All molecules contain the same atomic core region, "
    "consisting of the tetrahedral and octahedral Co centers and the "
    "three PO2R2 bridging ligands. The ligand exchange provides a broad range of "
    "exchange energies, ΔEJ, from +50 to −200 meV, with 80% of the "
    "ligands yielding a small ΔEJ < 10 meV."
)


def reader_Codimer(p):
    df = pd.read_csv(p, index_col=0)
    # df2=pd.read_csv('/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/Co_dimer_data.csv',index_col=0)
    # df=pd.concat([df1, df2])
    # print(df)

    structures_FM = []
    structures_AFM = []
    # atoms=read(p,index=',')
    for row in tqdm(df.index):
        file_FM = (
            "/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/structures_xyz/"
            + str(df.loc[row, "xyz_filename_FM"])
        )
        structure_FM = read(file_FM)
        # structures_FM.append(structure_FM)
        # print(structure_FM)
        structure_FM.info["energy"] = df.loc[row, "E-FM(a.u.)"].item()
        structures_FM.append(structure_FM)
        # print(type(structure_FM.info['energy_FM']))
        file_AFM = (
            "/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/structures_xyz/"
            + str(df.loc[row, "xyz_filename_AFM"])
        )
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


"""
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/',
    file_format='folder',
    name_field=None,
    elements=['Co', 'C', 'O', 'H', 'Cl', 'P', 'N','S'],
    default_name='Codimer',
    reader=reader_Codimer,
    glob_string='*.xyz',
    verbose=True,
    generator=False
)
"""
configurations = load_data(
    file_path="/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/",
    file_format="folder",
    name_field=None,
    elements=None,
    default_name="Codimer training",
    reader=reader_Codimer,
    glob_string="Co_dimer_data.csv",
    verbose=True,
    generator=False,
)
# print(configurations)

configurations += load_data(
    file_path="/large_data",
    file_format="folder",
    name_field=None,
    elements=None,
    default_name="Codimer all",
    reader=reader_Codimer,
    glob_string="Co_dimer_data_all.csv",
    verbose=True,
    generator=False,
)

client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
# client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# property included electronic and dispersion energies, highest occupied molecular
#  orbital (HOMO) and lowest unoccupied molecular orbital (LUMO) energies,
# HOMO/LUMO gap, dipole moment, and natural charge of the metal center;
# GFN2-xTB polarizabilities are also provided. Need to decide what to add
# in the property setting

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "a.u."},
            "per-atom": {"field": "per-atom", "units": None},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
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


ids = list(
    client.insert_data(
        configurations,
        property_map=property_map,
        generator=False,
        verbose=True,
    )
)

all_co_ids, all_pr_ids = list(zip(*ids))


cs_regexes = {
    "Codimer training": "Configurations used in machine learning",
}

cs_names = ["training"]

cs_ids = []

for i, (regex, desc) in enumerate(cs_regexes.items()):
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
        ravel=True,
    ).tolist()

    print(f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7))

    cs_id = client.insert_configuration_set(co_ids, description=desc, name=cs_names[i])

    cs_ids.append(cs_id)


ds_id = client.insert_dataset(
    cs_ids=cs_ids,
    do_hashes=all_pr_ids,
    name=DS_NAME,
    authors=AUTHORS,
    links=LINKS,
    description=DS_DESC,
    resync=True,
    verbose=True,
)

if __name__ == "__main__":
    main(sys.argv[1:])

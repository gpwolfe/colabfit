#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration
import pandas as pd
from ase import Atoms
from ase.io import read
from ase.io.vasp import read_vasp
from ase.db import connect
from tqdm import tqdm
import numpy as np

# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase(
    "new_data_test_alexander",
    configuration_type=AtomicConfiguration,
    nprocs=4,
    drop_database=True,
)

# In[ ]:


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


# Loads data, specify reader function if not "usual" file format
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
    file_path="/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/",
    file_format="folder",
    name_field=None,
    elements=None,
    default_name="Codimer all",
    reader=reader_Codimer,
    glob_string="Co_dimer_data_all.csv",
    verbose=True,
    generator=False,
)

# In[ ]:


client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
# client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# In[ ]:

# property included electronic and dispersion energies, highest occupied molecular orbital (HOMO) and lowest unoccupied molecular orbital (LUMO) energies, HOMO/LUMO gap, dipole moment, and natural charge of the metal center; GFN2-xTB polarizabilities are also provided. Need to decide what to add in the property setting

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


# In[ ]:


def tform(c):
    c.info["per-atom"] = False


# In[ ]:


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


# matches to data CO "name" field
cs_regexes = {
    #    '.*':
    #        'DFT data set of 1081 ligand substitutions for the Co(II) dimer. The ligand exchange '\
    #        'provides a broad range of exchange energies, ΔEJ, from +50 to −200 meV, with 80% of the '\
    #        'ligands yielding a small ΔEJ < 10 meV.',
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


# In[ ]:


ds_id = client.insert_dataset(
    cs_ids=cs_ids,
    do_hashes=all_pr_ids,
    name="CoDimer_JPCA_2022",
    authors=[
        "Sijin Ren",
        " Eric Fonseca",
        "William Perry",
        "Hai-Ping Cheng",
        "Xiao-Guang Zhang",
        "Richard Hennig",
    ],
    links=[
        "https://pubs.acs.org/doi/10.1021/acs.jpca.1c08950",
        "https://archive.materialscloud.org/record/2021.214",
    ],
    description="a data set of 1081 Co(II) dimer molecules with the Co atoms in the high-spin "
    "state of S = 3/2. All molecules contain the same atomic core region, "
    "consisting of the tetrahedral and octahedral Co centers and the "
    "three PO2R2 bridging ligands. The ligand exchange provides a broad range of "
    "exchange energies, ΔEJ, from +50 to −200 meV, with 80% of the "
    "ligands yielding a small ΔEJ < 10 meV.",
    resync=True,
    verbose=True,
)

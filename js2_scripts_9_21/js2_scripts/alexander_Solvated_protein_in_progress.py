#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration
from tqdm import tqdm
import numpy as np
from ase import Atoms

# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase(
    "new_data_test",
    configuration_type=AtomicConfiguration,
    nprocs=1,
    drop_database=True,
)


# In[ ]:
# solvated_protein-check out README in data's directory
def reader_protein(p):
    atoms = []
    a = np.load(p)
    na = a["N"]
    z = a["Z"]
    e = a["E"]
    # print(e)
    r = a["R"]
    f = a["F"]
    d = a["D"]
    q = a["Q"]
    # print(q)
    print(na)
    print(r)
    for i in tqdm(range(len(na))):
        n = na[i]
        atom = Atoms(numbers=z[i, :n], positions=r[i, :n, :])
        # atom.info['energy']=e[i]
        atom.info["energy"] = float(e[i])
        atom.arrays["forces"] = f[i, :n, :]
        atom.info["dipole_moment"] = d[i]
        atom.info["charge"] = float(q[i])
        # print(atom.info['charge'])
        # print(atom.arrays['forces'])
        atoms.append(atom)
        # print(type (atom.info['charge']))
    return atoms


# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path="/large_data/new_raw_datasets/Solvated_protein_UnkeOliverMeuwly/",
    file_format="folder",
    name_field=None,
    elements=["C", "N", "O", "S", "H"],
    default_name="solvated_protein",
    reader=reader_protein,
    glob_string="solvated_protein_fragments.npz",
    verbose=True,
    generator=False,
)

"""
configurations += load_data(
    file_path='/colabfit/data/data/gubaev/AlNiTi/train_2nd_stage.cfg',
    file_format='cfg',
    name_field=None,
    elements=['Al', 'Ni', 'Ti'],
    default_name='train_2nd_stage',
    verbose=True,
    generator=False
)
"""

# In[ ]:
atomization_property_definition = {
    "property-id": "atomization-energy",
    "property-name": "atomization-energy",
    "property-title": "energy minus molecular reference energy",
    "property-description": "the difference between energy and molecular reference energy",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "the difference between energy and molecular reference energy",
    },
}

charge_property_definition = {
    "property-id": "charge",
    "property-name": "charge",
    "property-title": "total charge",
    "property-description": "total charge of the metal center",
    "charge": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "total charge of the metal center",
    },
}

dipole_property_definition = {
    "property-id": "dipole",
    "property-name": "dipole",
    "property-title": "dipole moment",
    "property-description": "measurement of the separation of two opposite electrical charges",
    "dipole": {
        "type": "float",
        "has-unit": True,
        "extent": [":"],
        "required": True,
        "description": "measurement of the separation of two opposite electrical charges",
    },
}


client.insert_property_definition(atomization_property_definition)
client.insert_property_definition(dipole_property_definition)
client.insert_property_definition(charge_property_definition)

# client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# In[ ]:


property_map = {
    #    'potential-energy': [{
    #        'energy':   {'field': 'energy',  'units': 'eV'},
    #        'per-atom': {'field': 'per-atom', 'units': None},
    #        '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "revPBE-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    #    'cauchy-stress': [{
    #    'stress':   {'field': 'virial',  'units': 'GPa'},
    #                '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }],
    "atomization-energy": [
        {
            "energy": {"field": "atomization_energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "revPBE-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    "dipole": [
        {
            "dipole": {"field": "dipole_moment", "units": "eAng"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "revPBE-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    "charge": [
        {
            "charge": {"field": "charge", "units": "e"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "revPBE-D3(BJ)/def2-TZVP"},
            },
        }
    ],
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
    ".*": "In total, the dataset provides reference energies, forces, and dipole "
    "moments for 2731180 structures calculated at the revPBE-D3(BJ)/def2-TZVP level of "
    "theory using the ORCA 4.0.1 code.",
    #    'train_1st_stage':
    #        'Configurations used in the first stage of training',
    #    'train_2nd_stage':
    #        'Configurations used in the second stage of training',
}

cs_names = ["all"]


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
    pr_hashes=all_pr_ids,
    name="solvated_protein",
    authors=["Unke, O. T.", "Meuwly, M."],
    links=[
        "https://arxiv.org/abs/1902.08408",
        "https://zenodo.org/record/2605372#.Y3K8guTMJEZ",
    ],
    description="The solvated protein fragments dataset probes many-body "
    'intermolecular interactions between "protein fragments" and water molecules, '
    "which are important for the description of many biologically relevant"
    "condensed phase systems. It contains structures for all possible"
    '"amons" (hydrogen-saturated covalently bonded fragments) of up to eight heavy '
    "atoms (C, N, O, S) that can be derived from chemical graphs of proteins "
    "containing the 20 natural amino acids connected via peptide bonds or disulfide "
    "bridges. For amino acids that can occur in different charge states due to (de-)"
    "protonation (i.e. carboxylic acids that can be  negatively charged or amines that "
    "can be positively charged), all possible structures with up to a total charge of +-2e "
    "are included. In total, the dataset provides reference energies, forces, and dipole "
    "moments for 2731180 structures calculated at the revPBE-D3(BJ)/def2-TZVP level of "
    "theory using the ORCA 4.0.1 code.",
    resync=True,
    verbose=True,
)

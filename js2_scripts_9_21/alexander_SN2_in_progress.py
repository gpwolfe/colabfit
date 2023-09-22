#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration


# from cfkit.database import MongoDatabase, load_data
# from cfkit.configuration import AtomicConfiguration

from tqdm import tqdm
import numpy as np
from ase import Atoms

# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase(
    "new_data_test_alexander",
    configuration_type=AtomicConfiguration,
    nprocs=4,
    drop_database=True,
)


# In[ ]:
# solvated_protein-check out README in data's directory
def reader_SN2(p):
    atoms = []
    a = np.load(p)
    na = a["N"]
    z = a["Z"]
    e = a["E"]
    r = a["R"]
    f = a["F"]
    d = a["D"]
    q = a["Q"]
    for i in tqdm(range(len(na))):
        n = na[i]
        atom = Atoms(numbers=z[i, :n], positions=r[i, :n, :])
        atom.info["energy"] = e[i]
        atom.arrays["forces"] = f[i, :n, :]
        # print (f[i,:n,:])
        atom.info["dipole_moment"] = d[i]
        atom.info["charge"] = q[i]
        atoms.append(atom)
    return atoms


# atomization energy, dipole charge to be added
# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path="/large_data/new_raw_datasets/SN2_UnkeOliverMeuwly/",
    file_format="folder",
    name_field=None,
    elements=["C", "F", "Cl", "Br", "H", "I"],
    default_name="SN2",
    reader=reader_SN2,
    glob_string="*.npz",
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
    # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
    #        '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    #    'cauchy-stress': [{
    #    'stress':   {'field': 'virial',  'units': 'GPa'},
    #                '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }]
    "atomization-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            "_metadata": {
                # 'software': {'value':'VASP'},
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    "dipole": [
        {
            "dipole": {"field": "dipole_moment", "units": "e*Ang"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    "charge": [
        {
            "charge": {"field": "charge", "units": "e"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
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

"""
#matches to data CO "name" field
cs_regexes = {
    '.*':
        'In total, the dataset provides reference energies, forces, and dipole moments for 452709 structures'\
        'calculated at the DSD-BLYP-D3(BJ)/def2-TZVP level of theory using the ORCA 4.0.1 code.',
#    'train_1st_stage':
#        'Configurations used in the first stage of training',
#    'train_2nd_stage':
#        'Configurations used in the second stage of training',
}

cs_names=['all']
"""

cs_ids = []

"""
for i, (regex, desc) in enumerate(cs_regexes.items()):
    co_ids = client.get_data(
        'configurations',
        fields='hash',
        query={'hash': {'$in': all_co_ids}, 'names': {'$regex': regex}},
        ravel=True
    ).tolist()

    print(f'Configuration set {i}', f'({regex}):'.rjust(22), f'{len(co_ids)}'.rjust(7))

    cs_id = client.insert_configuration_set(co_ids, description=desc,name=cs_names[i])

    cs_ids.append(cs_id)
"""

# In[ ]:


ds_id = client.insert_dataset(
    # cs_ids=cs_ids,
    do_hashes=all_pr_ids,
    name="SN2",
    authors=["Oliver T. Unke", "Markus Meuwly"],
    links=[
        "https://pubs.acs.org/doi/10.1021/acs.jctc.9b00181",
        "https://zenodo.org/record/2605341#.Y3MppeTMJEZ",
    ],
    description="This dataset probes chemical reactions of methyl halides "
    "with halide anions, i.e. X- + CH3Y -> CH3X +  Y-, and contains structures "
    "for all possible combinations of X,Y = F, Cl, Br, I. The dataset also includes "
    "various structures for several smaller molecules that can be formed in "
    "fragmentation reactions, such as CH3X, HX, CHX or CH2X- as well as geometries "
    "for H2, CH2, CH3+ and XY interhalogen compounds. In total, the dataset provides "
    "reference energies, forces, and dipole moments for 452709 structures"
    "calculated at the DSD-BLYP-D3(BJ)/def2-TZVP level of theory using the ORCA 4.0.1 code.",
    resync=True,
    verbose=True,
)

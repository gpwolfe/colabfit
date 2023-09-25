#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration


# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase(
    "new_data_test_alexander",
    configuration_type=AtomicConfiguration,
    nprocs=4,
    drop_database=True,
)


# In[ ]:

# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path="/large_data/new_raw_datasets_2.0/Iron_nanoparticle/convex_hull.xyz",
    file_format="xyz",
    name_field=None,
    elements=["Fe"],
    default_name="ironnano",
    verbose=True,
    generator=False,
)

# In[ ]:


free_property_definition = {
    "property-id": "free-energy",
    "property-name": "free-energy",
    "property-title": "molecular reference energy",
    "property-description": "enthalpy of formation",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "enthalpy of formation",
    },
}

client.insert_property_definition(free_property_definition)


client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')    unit for stress unclear, waiting to be comfirmed


# In[ ]:


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            "_metadata": {
                "method": {"value": "DFT/PBE"},
                "software": {"value": "VASP"},
                "ecut": {"value": "400 eV"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "method": {"value": "DFT/PBE"},
                "software": {"value": "VASP"},
                "ecut": {"value": "400 eV"},
            },
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "_metadata": {
                "method": {"value": "DFT/PBE"},
                "software": {"value": "VASP"},
                "ecut": {"value": "400 eV"},
            },
        }
    ],
    #    'cauchy-stress': [{
    #        'stress':   {'field': 'virial',  'units': 'GPa'},
    #                '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #   }],
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
#    '.*':
#        'The PES and electronic energy datatsets for 2-b, 3-b and 4-b CCSD(T) interaction energies. '\
#        'the numbers of CCSD(T) energies in the datasets are 71,892, 45,332 and 3692 for '\
#        'the 2-, 3- and 4-b interactions, respectively.',
#    '2b_data':
#        'dataset for 2-b CCSD(T)/CBS interaction energies.',
#    '3b_data':
#        'dataset for 3-b BSSE corrected CCSD(T)-F12a/aVTZ interaction energies.',
#    '4b_data':
#        'dataset for 4-b CCSD(T)-F12/haTZ interaction energies.',
}

cs_names=[]


cs_ids = []

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
    name="Fe_nano_arxiv2023",
    authors=["Richard Jana", "Miguel A. Caro"],
    links=[
        "https://arxiv.org/pdf/2302.13722.pdf",
        "https://zenodo.org/record/7632315#.ZEMB8M7MJEY",
    ],
    description="The energy, forces and virials for the atomic structures in training database were computed at the DFT level "
    "of theory using VASP. The PBE func-tional was used with standard PAW pseudopotentials "
    "for Fe (with 8 valence electrons, 4s23d6). The kinetic energy cutoff for plane waves was "
    "set to 400 eV and the energy threshold for convergence was 10−7 eV. All the DFT calculations "
    "were carried out with spin polariza-tion, which can describe collinear magnetism",
    resync=True,
    verbose=True,
)

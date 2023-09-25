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
    file_path="/large_data/new_raw_datasets_2.0/q-AQUA/2b_data-reformatted.xyz",
    file_format="xyz",
    name_field=None,
    elements=["H", "O"],
    default_name="2b_data",
    verbose=True,
    generator=False,
)


configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/q-AQUA/3b_data-reformatted.xyz",
    file_format="xyz",
    name_field=None,
    elements=["H", "O"],
    default_name="3b_data",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/q-AQUA/4b_data-reformatted.xyz",
    file_format="xyz",
    name_field=None,
    elements=["H", "O"],
    default_name="4b_data",
    verbose=True,
    generator=False,
)
# In[ ]:


client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
# client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# In[ ]:


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "Hartree"},
            "per-atom": {"field": "per-atom", "units": None},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            "_metadata": {
                "method": {"value": "CCSD(T)"},
            },
        }
    ],
    #    'atomic-forces': [{
    #        'forces':   {'field': 'forces',  'units': 'eV/Ang'},
    #            '_metadata': {
    #            'software': {'value':'ÆNET'},
    #        }
    #    }],
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


# matches to data CO "name" field
cs_regexes = {
    #    '.*':
    #        'The PES and electronic energy datatsets for 2-b, 3-b and 4-b CCSD(T) interaction energies. '\
    #        'the numbers of CCSD(T) energies in the datasets are 71,892, 45,332 and 3692 for '\
    #        'the 2-, 3- and 4-b interactions, respectively.',
    "2b_data": "dataset for 2-b CCSD(T)/CBS interaction energies.",
    "3b_data": "dataset for 3-b BSSE corrected CCSD(T)-F12a/aVTZ interaction energies.",
    "4b_data": "dataset for 4-b CCSD(T)-F12/haTZ interaction energies.",
}

cs_names = ["2 body", "3 body", "4 body"]


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
    name="q-AQUA",
    authors=[
        "Qi Yu",
        "Chen Qu",
        " Paul L. Houston",
        " Riccardo Conte",
        "Apurba Nandi",
        "Joel M. Bowman",
    ],
    links=[
        "https://pubs.acs.org/doi/10.1021/acs.jpclett.2c00966",
        "https://github.com/jmbowma/q-AQUA",
    ],
    description="The PES and electronic energy datatsets for 2-b, 3-b and 4-b CCSD(T) interaction energies. "
    "the numbers of CCSD(T) energies in the datasets are 71,892, 45,332 and 3692 for "
    "the 2-, 3- and 4-b interactions, respectively.",
    resync=True,
    verbose=True,
)

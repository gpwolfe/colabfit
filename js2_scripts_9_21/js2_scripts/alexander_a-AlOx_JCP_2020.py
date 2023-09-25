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
    file_path="/large_data/new_raw_datasets_2.0/a-AlOx/train.xyz",
    file_format="xyz",
    name_field=None,
    elements=["Al", "O"],
    default_name="training set",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/a-AlOx/additional.xyz",
    file_format="xyz",
    name_field=None,
    elements=["Al", "O"],
    default_name="reference DFT calculations",
    verbose=True,
    generator=False,
)

# In[ ]:


client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# In[ ]:


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "PBE"},
                "ecut": {"value": "550 eV"},
                "kpoint": {"value": "5×5×2 or 5×3×2"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "PBE"},
                "ecut": {"value": "550 eV"},
                "kpoint": {"value": "5×5×2 or 5×3×2"},
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


# matches to data CO "name" field
cs_regexes = {
    #    '.*':
    #        'The structural, vibrational, mechanical, and thermal properties of the '\
    #        'a-AlOx models were investigated. The obtained reference '\
    #        'database includes the DFT energies of 41 203 structures. '\
    #        'The supercell size of the AlOx reference structures varied from 24 to 132 atoms',
    "training set": "The DFT (VASP) calculation results of reference structures that were used for the training of neural network potential.",
    "reference DFT calculations": "Additional reference DFT calculations that author used for reference.",
}

cs_names = ["DFT calculation", "reference DFT calculations"]


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
    name="a-AlOx_JCP_2020",
    authors=["Wenwen Li", "Yasunobu Ando", "Satoshi Watanabe"],
    links=[
        "https://aip.scitation.org/doi/suppl/10.1063/5.0026289",
        "https://archive.materialscloud.org/record/2020.89",
    ],
    description="In this work, the ab initio calculations were performed"
    "with the Vienna Ab initio Simulation Package. The projector"
    "augmented wave method was used to treat the atomic core electrons,"
    "and the Perdew–Burke–Ernzerhof functional within the generalized "
    "gradient approximation was used to describe the electron–electron "
    "interactions.The cutoff energy for the plane-wave basis set was "
    "set to 550 eV during the ab initio calculation.The structural, "
    "vibrational, mechanical, and thermal properties of the "
    "a-AlOx models were investigated. Finally, the obtained reference "
    "database includes the DFT energies of 41 203 structures. "
    "The supercell size of the AlOx reference structures varied from 24 to 132 atoms",
    resync=True,
    verbose=True,
)

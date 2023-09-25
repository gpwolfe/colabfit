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
    nprocs=1,
    drop_database=True,
)


# In[ ]:

# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path="/large_data/new_raw_datasets_2.0/LML-retrain/bulk_MD_test.xyz",
    file_format="xyz",
    name_field=None,
    elements=["W"],
    default_name="CHON",
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


client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
client.insert_property_definition("/home/ubuntu/notebooks/cauchy-stress.json")

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


# In[ ]:
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            "_metadata": {
                "software": {"field": "nomad_program_name"},
                "method": {"field": "nomad_electronic_structure_method"},
                "method_functional": {"field": "nomad_XC_functionals"},
            },
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "_metadata": {
                "software": {"field": "nomad_program_name"},
                "method": {"field": "nomad_electronic_structure_method"},
                "method_functional": {"field": "nomad_XC_functionals"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "VASP"},
            },
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "GPa"},
            "_metadata": {
                "software": {"value": "VASP"},
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
    ".*": "This dataset of molecular structures was extracted from all available "
    "structures in the NOMAD Archive that only include C, H, O, and N using the "
    "NOMAD API. This dataset consists of 50.42% H, 30.41% C, 10.36% N, and 8.81% O "
    "and includes 96 804 atomic environments in 5217 structures.",
    #    'HYB_GGA_XC_B3LYP':
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
    name="CHON",
    authors=["Berk Onat", "Christoph Ortner", "James R. Kermode"],
    links=[
        "https://aip.scitation.org/doi/10.1063/5.0016005",
        "https://github.com/DescriptorZoo/sensitivity-dimensionality-results/tree/master/datasets",
    ],
    description="This dataset of molecular structures was extracted from all available "
    "structures in the NOMAD Archive that only include C, H, O, and N using the "
    "NOMAD API. This dataset consists of 50.42% H, 30.41% C, 10.36% N, and 8.81% O "
    "and includes 96 804 atomic environments in 5217 structures.",
    resync=True,
    verbose=True,
)

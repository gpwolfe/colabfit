#!/usr/bin/env python
# coding: utf-8

# In[ ]:

# TODO READ THIS: Data beign uploaded from Si_md.extxyz is half of a dataset obtained from a paper called 'Sensitivity and Dimensionality...' by Onat, Ortner, andn Kermode ( https://aip.scitation.org/doi/10.1063/5.0016005 ).
# Data downloaded from this is cited as data from 'Representations in neural network
# based empirical potentials' ( https://aip.scitation.org/doi/10.1063/1.4990503 ) as
# a colleague said that it is the same data. I am placing this note here as since the
# data in the supplementary information is not in a format I know how to read so I
# cannot confirm it. If a customer messages about this data, please see this and direct
# them using this info.

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration


# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase(
    "new_data_test_dylan",
    configuration_type=AtomicConfiguration,
    nprocs=1,
    drop_database=True,
)


# In[ ]:

# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path="/large_data/new_raw_datasets/Si_Berk/Si_md.extxyz",
    file_format="extxyz",
    name_field=None,
    elements=["Si"],
    default_name="Si",
    verbose=True,
    generator=False,
)


# In[ ]:i

# kinetic_energy, energy, forces, free_energy

client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
# client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')

kinetic_energy_property_definition = {
    "property-id": "kinetic-energy",
    "property-name": "kinetic-energy",
    "property-title": "kinetic-energy",
    "property-description": "kinetic energy",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "kinetic energy",
    },
}

client.insert_property_definition(kinetic_energy_property_definition)

free_energy_property_definition = {
    "property-id": "free-energy",
    "property-name": "free-energy",
    "property-title": "free-energy",
    "property-description": "free energy",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "free energy",
    },
}
client.insert_property_definition(free_energy_property_definition)


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "____"},  # TODO
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "____"},  # TODO
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "____"},  # TODO
            "_metadata": {
                "software": {"value": "____"},  # TODO
            },
        }
    ],
    "kinetic-energy": [
        {
            "energy": {"field": "kinetic_energy", "units": ""},  # TODO
            "_metadata": {
                "software": {"value": ""},  # TODO
            },
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": ""},  # TODO
            "_metadata": {
                "software": {"value": ""},  # TODO
            },
        }
    ],
}


# In[ ]:


def tform(c):
    c.info["per-atom"] = False  # TODO


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
    ".*": "Si data from the Si dataset",
}


cs_names = ["Si"]


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
    name="Si_AIP2017",
    authors=["E. D. Cubuk", "B. D. Malone", "B. Onat", "A. Waterland", "E. Kaxiras"],
    links=[
        "https://aip.scitation.org/doi/10.1063/1.4990503",
    ],
    description="Silicon dataset used to train machine learning models. Machine"
    "learning has the potential to revolutionize materials modeling due"
    "to its ability to efficiently approximate complex functions.",
    resync=True,
    verbose=True,
)

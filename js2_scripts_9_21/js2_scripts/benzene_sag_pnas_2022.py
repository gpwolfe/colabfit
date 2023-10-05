#!/usr/bin/env python
# coding: utf-8


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


# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/benzene_test_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H"],
    default_name="benzene_test_QE_PBE_TS",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/benzene_train_FPS_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H"],
    default_name="benzene_train_FPS_QE_PBE_TS",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/benzene_val_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H"],
    default_name="benzene_val_QE_PBE_TS",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/glycine_test_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H", "N", "O"],
    default_name="glycine_test_QE_PBE_TS",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/glycine_train_FPS_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H", "N", "O"],
    default_name="glycine_train_FPS_QE_PBE_TS",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/glycine_val_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H", "N", "O"],
    default_name="glycine_val_QE_PBE_TS",
    verbose=True,
    generator=False,
)

configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/succinic_acid_test_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H", "O"],
    default_name="succinic_acid_test_QE_PBE_TS",
    verbose=True,
    generator=False,
)
configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/succinic_acid_train_FPS_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H", "O"],
    default_name="succinic_acid_train_FPS_QE_PBE_TS",
    verbose=True,
    generator=False,
)
configurations += load_data(
    file_path="/large_data/new_raw_datasets_2.0/benzene_succinic_acid_glycine/succinic_acid_val_QE_PBE_TS.xyz",
    file_format="xyz",
    name_field=None,
    elements=["C", "H", "O"],
    default_name="succinic_acid_val_QE_PBE_TS",
    verbose=True,
    generator=False,
)


client.insert_property_definition("/home/ubuntu/notebooks/potential-energy.json")
client.insert_property_definition("/home/ubuntu/notebooks/atomic-forces.json")
client.insert_property_definition("/home/ubuntu/notebooks/cauchy-stress.json")


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            # For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            "_metadata": {
                "software": {"value": "Quantum Espresso v6.3"},
                "method": {"value": "PBE-TS"},
                "kpoint": {
                    "value": "Monkhorst-Pack k-point grid with a maximum spacing of 0.06 × 2π Å−1"
                },
                "ecut": {"value": "100 Rydberg"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "Quantum Espresso v6.3"},
                "method": {"value": "PBE-TS"},
                "kpoint": {
                    "value": "Monkhorst-Pack k-point grid with a maximum spacing of 0.06 × 2π Å−1"
                },
                "ecut": {"value": "100 Rydberg"},
            },
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/Ang^3"},
            "_metadata": {
                "software": {"value": "Quantum Espresso v6.3"},
                "method": {"value": "PBE-TS"},
                "kpoint": {
                    "value": "Monkhorst-Pack k-point grid with a maximum spacing of 0.06 × 2π Å−1"
                },
                "ecut": {"value": "100 Rydberg"},
            },
        }
    ],
}


def tform(c):
    c.info["per-atom"] = False


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
    #        'DFT reference energies and forces were calculated using '\
    #        'Quantum Espresso v6.3. The calculations were per-formed with '\
    #        'the semi-local PBE exchange-correlation functional, the '\
    #        'Tkatchenko-Scheffler (TS) disper-sion correction, optimised '\
    #        'norm-conserving Van-derbilt pseudopotentials,',
    "benzene_test_QE_PBE_TS": "benzene test datasets.",
    "benzene_train_FPS_QE_PBE_TS": "benzene datasets for training.",
    "benzene_val_QE_PBE_TS": "benzene datasets for validation.",
    "glycine_test_QE_PBE_TS": "glycine test datasets.",
    "glycine_train_FPS_QE_PBE_TS": "glycine datasets for training.",
    "glycine_val_QE_PBE_TS": "glycine datasets for validation.",
    "succinic_acid_test_QE_PBE_TS": "succinic test datasets.",
    "succinic_acid_train_FPS_QE_PBE_TS": "succinic datasets for training.",
    "succinic_acid_val_QE_PBE_TS": "succinic datasets for validation.",
}

cs_names = [
    "benzene test",
    "benzene train",
    "benzene validation",
    "glycine test",
    "glycine train",
    "glycine validation",
    "succinic test",
    "succinic train",
    "succinic validation",
]


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
    name="benzene_succinic_acid_glycine_TS_PNAS_2022",
    authors=["Venkat Kapil", "Edgar A. Engel"],
    links=[
        "https://www.pnas.org/doi/10.1073/pnas.2111769119#sec-4",
        "https://archive.materialscloud.org/record/2021.51",
        "https://github.com/venkatkapil24/data_molecular_fluctuations",
    ],
    description="DFT reference energies and forces were calculated using "
    "Quantum Espresso v6.3. The calculations were per-formed with "
    "the semi-local PBE exchange-correlation functional, the "
    "Tkatchenko-Scheffler (TS) disper-sion correction, optimised "
    "norm-conserving Van-derbilt pseudopotentials, a "
    "Monkhorst-Pack k-point grid with a maximum spacing of 0.06 × 2π "
    "Å−1, and a plane-wave energy cut-off of 100 Rydberg for the wavefunction.",
    resync=True,
    verbose=True,
)

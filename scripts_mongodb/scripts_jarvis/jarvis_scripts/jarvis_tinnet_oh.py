"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

Keys
['atoms',
'ead',
'id']
"""


import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client


DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "tinnet_OH.json"
DS_NAME = "JARVIS_TinNet_OH"
DS_DESC = (
    "The JARVIS_TinNet dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the TinNet-OH dataset: a collection assembled to train a "
    "machine learning model for the purposes of assisting catalyst design by "
    "predicting chemical reactivity of transition-metal surfaces. The adsorption "
    "systems contained in this dataset consist of {111}-terminated metal surfaces. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1038/s41467-021-25639-8"
DATA_LINK = "https://figshare.com/ndownloader/files/40934285"
OTHER_LINKS = [
    "https://jarvis.nist.gov/",
    "https://github.com/hlxin/tinnet/tree/master",
]
LINKS = [
    "https://figshare.com/ndownloader/files/40934285",
    "https://jarvis.nist.gov/",
    "https://github.com/hlxin/tinnet/tree/master",
    "https://doi.org/10.1038/s41467-021-25639-8",
]
AUTHORS = [
    "Shih-Han Wang",
    "Hemanth Somarajan Pillai",
    "Siwen Wang",
    "Luke E. K. Achenie",
    "Hongliang Xin",
]
ELEMENTS = None

PI_MD = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "kpoints": "6 x 6 x 1",
            "kpoints-scheme": "Monkhorst-Pack",
            "cutoff": {"value": 500, "units": "eV"},
        }
    },
}

PROPERTY_MAP = {
    "adsorption-energy": [
        {
            "energy": {"field": "ead", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


# with open("formation_energy.json", "r") as f:
#     formation_energy_pd = json.load(f)
with open("adsorption_energy.json", "r") as f:
    adsorption_energy_pd = json.load(f)


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
        data = data
    configs = []
    for i, row in enumerate(data):
        atoms = row.pop("atoms")
        if atoms["cartesian"] is True:
            config = AtomicConfiguration(
                positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        else:
            config = AtomicConfiguration(
                scaled_positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        config.info["name"] = f"{fp.stem}_{i}"
        for key, val in row.items():
            if isinstance(val, str) and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, dict) and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (isinstance(val, float) or isinstance(val, int)) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
            else:
                pass
        configs.append(config)
    return configs


def main(argv):
    client = get_client(argv)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB,
        generator=False,
    )

    client.insert_property_definition(adsorption_energy_pd)
    # client.insert_property_definition(atomic_forces_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        data_license=LICENSE,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


CO_KEYS = [
    # 'atoms',
    # "ead",
    "id",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

keys:

"total_energy"
"forces",
"stresses",
"jid",
"atoms"
"""


import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "id_prop.json"
DS_NAME = "JARVIS_ALIGNN_FF"
DS_DESC = (
    "The JARVIS_ALIGNN_FF dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset is a subset of "
    "the JARVIS DFT dataset, filtered to contain just the first, last, middle, "
    "maximum energy and minimum energy structures. Additionally, calculation run "
    "snapshots are filtered for uniqueness, and the dataset contains only perfect "
    "structures. DFT energies, stresses and forces in this dataset were used to train "
    "an atomisitic line graph neural network (ALIGNN)-based FF model. JARVIS is a set "
    "of tools and datasets built to meet current materials design challenges."
)

PUBLICATION = "https://doi.org/10.1039/D2DD00096B"
DATA_LINK = "https://ndownloader.figshare.com/files/38522315"
OTHER_LINKS = [
    "https://github.com/usnistgov/alignn",
    "https://jarvis.nist.gov/",
]
LINKS = [
    "https://ndownloader.figshare.com/files/38522315",
    "https://jarvis.nist.gov/",
    "https://github.com/usnistgov/alignn",
    "https://doi.org/10.1039/D2DD00096B",
]
AUTHORS = [
    "Kamal Choudhary",
    "Brian DeCost",
    "Lily Major",
    "Keith Butler",
    "Jeyan Thiyagalingam",
    "Francesca Tavazza",
]
ELEMENTS = None


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "total_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"field": "DFT-OptB88vdW"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"field": "DFT-OptB88vdW"},
            },
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stresses", "units": "eV"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"field": "DFT-OptB88vdW"},
            },
        }
    ],
}


with open("formation_energy.json", "r") as f:
    formation_energy_pd = json.load(f)
with open("band_gap.json", "r") as f:
    band_gap_pd = json.load(f)


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
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
                config.info[key] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key] = val
            elif isinstance(val, dict) and all([v != "na" for v in val.values()]):
                config.info[key] = val
            elif isinstance(val, float) or isinstance(val, int):
                config.info[key] = val
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

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(atomic_forces_pd)

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
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


CO_KEYS = [
    # "total_energy",
    # "forces",
    # "stresses",
    "jid",
    # "atoms"
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

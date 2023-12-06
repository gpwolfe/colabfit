"""
author: gpwolfe

File notes
----------
The "energy" key reported here has the same value for all configurations
Additionally, AGRA-CHO, AGRA-CO and AGRA-COOH share the same value:
-106.79480578611286
This is therefore being placed in metadata rather than a PI

Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
fund = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
optb88vdw_total_energy (dft_3d)
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

keys:

['atoms', 'ead', 'energy', 'id']
"""


import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client

from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "AGRA_O.json"
DS_NAME = "JARVIS_AGRA_O"
DS_DESC = (
    "The JARVIS_AGRA_O dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) DFT database. This dataset contains "
    "data from the training set for the oxygen reduction reaction (ORR) dataset "
    "from Batchelor et al., as used in the automated graph representation algorithm "
    "(AGRA) training dataset: a collection of DFT training data for training a graph "
    "representation method to extract the local chemical environment of metallic "
    "surface adsorption sites.  Bulk calculations were "
    "performed with k-point = 8 x 8 x 4. Training adsorption energies were calculated "
    "on slabs, k-point = 4 x 4 x 1, while testing energies used k-point = 3 x 3 x 1. "
    "JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges."
)
PUBLICATION = "https://doi.org/10.1016/j.joule.2018.12.015"
DATA_LINK = "https://figshare.com/ndownloader/files/41923284"
OTHER_LINKS = [
    "https://github.com/Feugmo-Group/AGRA",
    "https://jarvis.nist.gov/",
    "https://doi.org/10.1063/5.0140487",
]
LINKS = [
    "https://github.com/Feugmo-Group/AGRA",
    "https://jarvis.nist.gov/",
    "https://figshare.com/ndownloader/files/41923284",
    "https://doi.org/10.1063/5.0140487",
    "https://doi.org/10.1016/j.joule.2018.12.015",
]
AUTHORS = [
    "Thomas A.A. Batchelor",
    "Jack K. Pedersen",
    "Simon H. Winther",
    "Ivano E. Castelli",
    "Karsten W. Jacobsen",
    "Jan Rossmeisl",
]
ELEMENTS = None
PI_MD = {
    "software": {"value": "GPAW"},
    "method": {"value": "DFT-rPBE"},
    "input": {
        "value": {
            "encut": {"value": 400, "units": "eV"},
            "kpoints-scheme": "Monkhorst-Pack",
        },
    },
}

PROPERTY_MAP = {
    "adsorption-energy": [
        {
            "energy": {"field": "ead", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


with open("adsorption_energy.json", "r") as f:
    adsorption_energy_pd = json.load(f)
# with open("band_gap.json", "r") as f:
#     band_gap_pd = json.load(f)


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

    client.insert_property_definition(adsorption_energy_pd)
    client.insert_property_definition(potential_energy_pd)

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
    # "atoms",
    # "ead",
    # "energy",
    "id",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

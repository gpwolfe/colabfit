"""
author: gpwolfe

File notes
----------
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

2DMatPedia keys
[
"atoms",
"bandgap",
"energy_per_atom",
"exfoliation_energy_per_atom",
"material_id",
"source_id",
"thermo",
"total_magnetization",
]

"""

import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client

from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "twodmatpd.json"
DS_NAME = "JARVIS-2DMatPedia"
DS_DESC = (
    "The JARVIS-2DMatPedia dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This subset contains "
    "configurations with 2D materials from the 2DMatPedia database, generated through "
    "two methods: a top-down exfoliation approach, using structures of bulk "
    "materials from the Materials Project database; and a bottom-up approach, "
    "replacing each element in a 2D material with another from the same group "
    "(according to column number). JARVIS is a set of tools and datasets built to meet "
    "current materials design challenges."
)

LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1038/s41597-019-0097-3"
DATA_LINK = "https://ndownloader.figshare.com/files/26789006"
OTHER_LINKS = ["https://jarvis.nist.gov/"]
LINKS = [
    "https://doi.org/10.1038/s41597-019-0097-3",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/26789006",
]
AUTHORS = [
    "Jun Zhou",
    "Lei Shen",
    "Miguel Dias Costa",
    "Kristin A. Persson",
    'Shyue Ping Ong", "Patrick Huck',
    "Yunhao Lu",
    "Xiaoyang Ma",
    "Yiming Chen",
    "Hanmei Tang",
    "Yuan Ping Feng",
]
ELEMENTS = None
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-vdW-optB88"},
    "input": {
        "value": {
            "encut": {"value": 520, "units": "eV"},
            "ediffg": 1 * 10e-4,
            "isif": 3,
        }
    },
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy_per_atom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_MD,
        },
        # {
        #     "energy": {"field": "energy", "units": "eV"},
        #     "per-atom": {"value": False, "units": None},
        #     "_metadata": PI_MD,
        # },
    ],
    "band-gap": [
        {
            "energy": {"field": "bandgap", "units": "eV"},
            "_metadata": PI_MD,
        },
    ],
}


# with open("formation_energy.json", "r") as f:
#     formation_energy_pd = json.load(f)
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
        # energy = row.get("thermo")
        # if energy:
        #     energy = energy.get("energy")
        #     if energy:
        #         config.info["energy"] = energy
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

    # client.insert_property_definition(formation_energy_pd)
    client.insert_property_definition(band_gap_pd)
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
        data_license=LICENSE,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


CO_KEYS = [
    # "atoms",
    # "bandgap",
    # "energy_per_atom",
    "exfoliation_energy_per_atom",
    "material_id",
    "source_id",
    "thermo",
    "total_magnetization",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

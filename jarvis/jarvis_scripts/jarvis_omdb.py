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

Keys
['atoms',
'bandgap',
'cod_id']
"""


import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client

# from colabfit.tools.property_definitions import potential_energy_pd


DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "omdbv1.json"
DS_NAME = "JARVIS_OMDB"
DS_DESC = (
    "The JARVIS_OMDB dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the Organic Materials Database (OMDB): a dataset of 12,500 "
    "crystal materials for the purpose of training models for the prediction of "
    "properties for complex and lattice-periodic organic crystals with large numbers "
    "of atoms per unit cell. Dataset covers 69 space groups, 65 elements; averages 82 "
    "atoms per unit cell. "
    "This dataset also includes classical force-field inspired descriptors (CFID) for "
    "each configuration. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LINKS = [
    "https://doi.org/10.1002/qute.201900023",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/28501761",
]
AUTHORS = [
    "Bart Olsthoorn",
    "R. Matthias Geilhufe",
    "Stanislav S. Borysov",
    "Alexander V. Balatsky",
]
ELEMENTS = None


PROPERTY_MAP = {
    "band-gap": [
        {
            "energy": {"field": "bandgap", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "k-point": {"value": "6x6x6"},
            },
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

    # client.insert_property_definition(free_energy_pd)
    # client.insert_property_definition(formation_energy_pd)
    client.insert_property_definition(band_gap_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
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
        verbose=True,
    )


CO_KEYS = [
    # 'atoms',
    # 'bandgap',
    "cod_id"
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

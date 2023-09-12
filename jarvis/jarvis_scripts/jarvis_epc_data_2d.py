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

['Tc',
 'a2F',
 'a2F_original_x',
 'a2F_original_y',
 'atoms',
 'cfid',
 'dos_info',
 'formation_energy_peratom',
 'formula',
 'jid',
 'lamb',
 'press',
 'spg_number',
 'stability',
 'wlog']
"""

from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

# from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path("jarvis_json/")
GLOB = "jarvis_epc_data_2d.json"
DS_NAME = "JARVIS_EPC_2D"
DS_DESC = (
    "The JARVIS_EPC_2D dataset is part of the joint automated repository "
    "for various integrated simulations (JARVIS) DFT database. This subset contains "
    "configurations sourced from the JARVIS-DFT-2D dataset, rerelaxed with Quantum "
    "ESPRESSO. JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges."
)

LINKS = [
    "https://figshare.com/ndownloader/files/38950433",
    "https://jarvis.nist.gov/",
    "https://doi.org/10.1021/acs.nanolett.2c04420",
]
AUTHORS = [
    "Daniel Wines",
    "Kamal Choudhary",
    "Adam J. Biacchi",
    "Kevin F. Garrity",
    "Francesca Tavazza",
]
ELEMENTS = None


PROPERTY_MAP = {
    "formation-energy": [
        {
            "energy": {"field": "formation_energy_peratom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": {
                "software": {"value": "Quantum ESPRESSO"},
                "method": {"value": "PBEsol"},
                "ecut": {"field": "610 eV"},
            },
        }
    ],
}


with open("formation_energy.json", "r") as f:
    formation_energy_pd = json.load(f)
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
            if type(val) == str and val != "na" and len(val) > 0:
                config.info[key] = val
            elif type(val) == list and len(val) > 0 and any([x != "" for x in val]):
                config.info[key] = val
            elif type(val) == dict and all([v != "na" for v in val.values()]):
                config.info[key] = val
            elif type(val) == float or type(val) == int:
                config.info[key] = val
            else:
                pass
        configs.append(config)
    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

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

    client.insert_property_definition(formation_energy_pd)
    # client.insert_property_definition(band_gap_pd)
    # client.insert_property_definition(potential_energy_pd)

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
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


CO_KEYS = [
    "Tc",
    "a2F",
    "a2F_original_x",
    "a2F_original_y",
    #  'atoms',
    "cfid",
    "dos_info",
    #  'formation_energy_peratom',
    "formula",
    "jid",
    "lamb",
    "press",
    "spg_number",
    "stability",
    "wlog",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

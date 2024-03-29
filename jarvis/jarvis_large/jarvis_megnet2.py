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

megnet2 property keys
'atoms',
'id',
'formation_energy_per_atom'
"""


from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase


# from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "megnet-mp-2019-04-01.json"
DS_NAME = "JARVIS_MEGNet2"
DS_DESC = (
    "The JARVIS-MEGNet2 dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This subset contains 133K "
    "materials with formation energy from the Materials "
    "Project, as used in the training of the MEGNet ML model. JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges."
)


PUBLICATION = "https://doi.org/10.1021/acs.chemmater.9b01294"
DATA_LINK = "https://ndownloader.figshare.com/files/28332741"
OTHER_LINKS = ["https://jarvis.nist.gov/"]
LINKS = [
    "https://doi.org/10.1021/acs.chemmater.9b01294",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/28332741",
]
AUTHORS = ["Chi Chen", "Weike Ye", "Yunxing Zuo", "Chen Zheng", "Shyue Ping Ong"]
ELEMENTS = None
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
}

PROPERTY_MAP = {
    "formation-energy": [
        {
            "energy": {"field": "formation_energy_per_atom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
    # "band-gap": [
    #     {
    #         "energy": {"field": "gap-pbe", "units": "eV"},
    #         "_metadata": PI_MD,
    #     },
    # ],
}


with open("formation_energy.json", "r") as f:
    formation_energy_pd = json.load(f)
with open("band_gap.json", "r") as f:
    band_gap_pd = json.load(f)


def get_client(argv):
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    return client


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
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, dict) and all([v != "na" for v in val.values()]):
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

    client.insert_property_definition(formation_energy_pd)
    # client.insert_property_definition(band_gap_pd)
    # client.insert_property_definition(potential_energy_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            # co_md_map=CO_METADATA,
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


# CO_KEYS = ['atoms', 'id', 'formation_energy_per_atom']
# CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

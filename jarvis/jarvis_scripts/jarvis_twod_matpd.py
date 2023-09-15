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

from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

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
    "ecut": {"value": "520 eV"},
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
    # "formation-energy": [
    #     {
    #         "energy": {"field": "e_form", "units": "eV"},
    #         "per-atom": {"value": True, "units": None},
    #         "_metadata": PI_MD,
    #     }
    # ],
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
            if type(val) == str and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif type(val) == list and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif type(val) == dict and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (type(val) == float or type(val) == int) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
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

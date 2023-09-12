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

keys:

['atoms',
 'band_gap',
 'density',
 'desc',
 'diel',
 'e_above_hull',
 'elasticity',
 'elements',
 'energy',
 'energy_per_atom',
 'formation_energy_per_atom',
 'full_formula',
 'hubbards',
 'icsd_id',
 'icsd_ids',
 'id',
 'is_compatible',
 'is_hubbard',
 'material_id',
 'nelements',
 'nsites',
 'oxide_type',
 'piezo',
 'pretty_formula',
 'spacegroup',
 'tags',
 'task_ids',
 'total_magnetization',
 'unit_cell_formula',
 'volume']
"""

from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "all_mp.json"
DS_NAME = "JARVIS_Materials_Project_2020"
DS_DESC = (
    "The JARVIS_Materials_Project_2020 dataset is part of the joint automated "
    "repository for various integrated simulations (JARVIS) DFT database. "
    "This subset contains 127,000 configurations of 3D materials from the Materials "
    "Project database. "
    "JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges. JARVIS-DFT "
    "is the portion of the JARVIS database based on DFT calculations."
)

LINKS = [
    "https://doi.org/10.1063/1.4812323",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/26791259",
]
AUTHORS = [
    "Anubhav Jain",
    "Shyue Ping Ong",
    "Geoffroy Hautier",
    "Wei Chen",
    "William Davidson Richards",
    "Stephen Dacek",
    "Shreyas Cholia",
    "Dan Gunter",
    "David Skinner",
    "Gerbrand Ceder",
    "Kristin A. Persson",
]
ELEMENTS = None


PROPERTY_MAP = {
    "formation-energy": [
        {
            "energy": {"field": "formation_energy_per_atom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                # "method": {"field": "method"},
                # "ecut": {"field": "encut"},
            },
        }
    ],
    "band-gap": [
        {
            "energy": {"field": "band_gap", "units": "eV"},
            "_metadata": {
                # "method": {"value": "DFT-TBmBJ"},
                "software": {"value": "VASP"},
            },
        },
    ],
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                # "method": {"value": "DFT-OptB88vdW"},
                # "ecut": {"field": "encut"},
            },
        },
        {
            "energy": {"field": "energy_per_atom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                # "method": {"value": "DFT-OptB88vdW"},
                # "ecut": {"field": "encut"},
            },
        },
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
    # "band_gap",
    "density",
    "desc",
    "diel",
    "e_above_hull",
    "elasticity",
    "elements",
    # "energy",
    # "energy_per_atom",
    # "formation_energy_per_atom",
    "full_formula",
    "hubbards",
    "icsd_id",
    "icsd_ids",
    "id",
    "is_compatible",
    "is_hubbard",
    "material_id",
    "nelements",
    "nsites",
    "oxide_type",
    "piezo",
    "pretty_formula",
    "spacegroup",
    "tags",
    "task_ids",
    "total_magnetization",
    "unit_cell_formula",
    "volume",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

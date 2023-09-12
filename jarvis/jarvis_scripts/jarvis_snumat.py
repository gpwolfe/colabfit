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
['Band_gap_GGA',
 'Band_gap_GGA_optical',
 'Band_gap_HSE',
 'Band_gap_HSE_optical',
 'Direct_or_indirect',
 'Direct_or_indirect_HSE',
 'ICSD_number',
 'Magnetic_ordering',
 'SNUMAT_id',
 'SOC',
 'Space_group_rlx',
 'Structure_rlx',
 'atoms']
"""

from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

from colabfit.tools.property_definitions import potential_energy_pd


DATASET_FP = Path("jarvis_json_zips/")
GLOB = "snumat.json"
DS_NAME = "JARVIS_SNUMAT"
DS_DESC = (
    "The JARVIS_SNUMAT dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "band gap data for >10,000 materials, computed using a hybrid functional and "
    "considering the stable magnetic ordering. Structure relaxation and band edges "
    "are obtained using the PBE XC functional; band gap energy is subsequently "
    "obtained using the HSE06 hybrid functional. Optical and fundamental band gap "
    "energies are included. Some gap energies are recalculated by including spin-orbit "
    'coupling. These are noted in the band gap metadata as "SOC=true". '
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LINKS = [
    "https://doi.org/10.1038/s41597-020-00723-8",
    "https://jarvis.nist.gov/",
    "https://www.snumat.com/",
    "https://ndownloader.figshare.com/files/38521736",
]
AUTHORS = [
    "Sangtae Kim",
    "Miso Lee",
    "Changho Hong",
    "Youngchae Yoon",
    "Hyungmin An",
    "Dongheon Lee",
    "Wonseok Jeong",
    "Dongsun Yoo",
    "Youngho Kang",
    "Yong Youn",
    "Seungwu Han",
]
ELEMENTS = None


PROPERTY_MAP = {
    "band-gap": [
        {
            "energy": {"field": "Band_gap_GGA", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "fundamental_optical": {"value": "fundamental"},
                "direct_indirect": {"field": "Direct_or_indirect"},
                "SOC": {"field": "SOC"},
            },
        },
        {
            "energy": {"field": "Band_gap_GGA_optical", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "fundamental_optical": {"value": "optical"},
                "direct_indirect": {"field": "Direct_or_indirect"},
                "SOC": {"field": "SOC"},
            },
        },
        {
            "energy": {"field": "Band_gap_HSE", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-HSE06"},
                "fundamental_optical": {"value": "fundamental"},
                "direct_indirect": {"field": "Direct_or_indirect_HSE"},
                "SOC": {"field": "SOC"},
            },
        },
        {
            "energy": {"field": "Band_gap_HSE_optical", "units": "eV"},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-HSE06"},
                "fundamental_optical": {"value": "optical"},
                "direct_indirect": {"field": "Direct_or_indirect_HSE"},
                "SOC": {"field": "SOC"},
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
        elements = [x if x != "D" else "H" for x in atoms["elements"]]
        if atoms["cartesian"] is True:
            config = AtomicConfiguration(
                positions=atoms["coords"],
                symbols=elements,
                cell=atoms["lattice_mat"],
            )
        else:
            config = AtomicConfiguration(
                scaled_positions=atoms["coords"],
                symbols=elements,
                cell=atoms["lattice_mat"],
            )
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

    # client.insert_property_definition(free_energy_pd)
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
    # "Band_gap_GGA",
    # "Band_gap_GGA_optical",
    # "Band_gap_HSE",
    # "Band_gap_HSE_optical",
    # "Direct_or_indirect",
    # "Direct_or_indirect_HSE",
    "ICSD_number",
    "Magnetic_ordering",
    "SNUMAT_id",
    # "SOC",
    "Space_group_rlx",
    "Structure_rlx",
    # "atoms",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

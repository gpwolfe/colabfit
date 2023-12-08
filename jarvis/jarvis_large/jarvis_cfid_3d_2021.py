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
 'avg_elec_mass',
 'avg_hole_mass',
 'bulk_modulus_kv',
 'crys',
 'density',
 'desc',
 'dfpt_piezo_max_dielectric',
 'dfpt_piezo_max_dielectric_electronic',
 'dfpt_piezo_max_dielectric_ionic',
 'dfpt_piezo_max_dij',
 'dfpt_piezo_max_eij',
 'dimensionality',
 'effective_masses_300K',
 'efg',
 'ehull',
 'elastic_tensor',
 'encut',
 'epsx',
 'epsy',
 'epsz',
 'exfoliation_energy',
 'formation_energy_peratom',
 'formula',
 'func',
 'hse_gap',
 'icsd',
 'jid',
 'kpoint_length_unit',
 'magmom_oszicar',
 'magmom_outcar',
 'max_efg',
 'max_ir_mode',
 'maxdiff_bz',
 'maxdiff_mesh',
 'mbj_bandgap',
 'mepsx',
 'mepsy',
 'mepsz',
 'min_ir_mode',
 'modes',
 'n-Seebeck',
 'n-powerfact',
 'nat',
 'ncond',
 'nkappa',
 'optb88vdw_bandgap',
 'optb88vdw_total_energy',
 'p-Seebeck',
 'p-powerfact',
 'pcond',
 'pkappa',
 'poisson',
 'raw_files',
 'reference',
 'search',
 'shear_modulus_gv',
 'slme',
 'spg',
 'spg_number',
 'spg_symbol',
 'spillage',
 'typ',
 'xml_data_link']
"""


from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "cfid_3d-8-18-2021.json"
DS_NAME = "JARVIS_CFID_3D_8_18_2022"
DS_DESC = (
    "The JARVIS_CFID_3D_8_18_2022 dataset is part of the joint automated repository "
    "for various integrated simulations (JARVIS) DFT database. This subset contains "
    "configurations of 3D materials. JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges."
)

PUBLICATION = "https://doi.org/10.1038/s41524-020-00440-1"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.6815699"
OTHER_LINKS = ["https://jarvis.nist.gov/"]
LINKS = [
    "https://doi.org/10.1038/s41524-020-00440-1",
    "https://jarvis.nist.gov/",
    "https://doi.org/10.6084/m9.figshare.6815699",
]
AUTHORS = [
    "Kamal Choudhary",
    "Kevin F. Garrity",
    "Andrew C. E. Reid",
    "Brian DeCost",
    "Adam J. Biacchi",
    "Angela R. Hight Walker",
    "Zachary Trautt",
    "Jason Hattrick-Simpers",
    "A. Gilad Kusne",
    "Andrea Centrone",
    "Albert Davydov",
    "Jie Jiang",
    "Ruth Pachter",
    "Gowoon Cheon",
    "Evan Reed",
    "Ankit Agrawal",
    "Xiaofeng Qian",
    "Vinit Sharma",
    "Houlong Zhuang",
    "Sergei V. Kalinin",
    "Bobby G. Sumpter",
    "Ghanshyam Pilania",
    "Pinar Acar",
    "Subhasish Mandal",
    "Kristjan Haule",
    "David Vanderbilt",
    "Karin Rabe",
    "Francesca Tavazza",
]
ELEMENTS = None


PROPERTY_MAP = {
    "formation-energy": [
        {
            "energy": {"field": "formation_energy_peratom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"field": "method"},
                "input": {"field": "input"},
            },
        }
    ],
    "band-gap": [
        {
            "energy": {"field": "mbj_bandgap", "units": "eV"},
            "_metadata": {
                "method": {"value": "DFT-TBmBJ"},
                "software": {"value": "VASP"},
                "input": {"field": "input"},
            },
        },
        # {
        #     "energy": {"field": "optb88vdw_bandgap", "units": "eV"},
        #     "_metadata": {
        #         "method": {"value": "DFT-OptB88vdW"},
        #         "software": {"value": "VASP"},
        #     },
        # },
    ],
    "potential-energy": [
        {
            "energy": {"field": "optb88vdw_total_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-OptB88vdW"},
                "input": {"field": "input"},
            },
        }
    ],
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
        config.info["method"] = f"DFT-{row.pop('func')}"
        encut = config.info.get("encut")
        if encut is not None:
            config.info["input"] = {
                "encut": {"value": encut, "units": "eV"},
                "ediff": {"value": 10e-7, "units": "eV"},
                "ediffg": {"value": 0.0001, "units": "eV/angstrom"},
            }
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
    # 'atoms',
    "avg_elec_mass",
    "avg_hole_mass",
    "bulk_modulus_kv",
    "crys",
    "density",
    "desc",
    "dfpt_piezo_max_dielectric",
    "dfpt_piezo_max_dielectric_electronic",
    "dfpt_piezo_max_dielectric_ionic",
    "dfpt_piezo_max_dij",
    "dfpt_piezo_max_eij",
    "dimensionality",
    "effective_masses_300K",
    "efg",
    "ehull",
    "elastic_tensor",
    "encut",
    "epsx",
    "epsy",
    "epsz",
    "exfoliation_energy",
    "formation_energy_peratom",
    "formula",
    "func",
    "hse_gap",
    "icsd",
    "jid",
    "kpoint_length_unit",
    "magmom_oszicar",
    "magmom_outcar",
    "max_efg",
    "max_ir_mode",
    "maxdiff_bz",
    "maxdiff_mesh",
    "mbj_bandgap",
    "mepsx",
    "mepsy",
    "mepsz",
    "min_ir_mode",
    "modes",
    "n-Seebeck",
    "n-powerfact",
    "nat",
    "ncond",
    "nkappa",
    "optb88vdw_bandgap",
    "optb88vdw_total_energy",
    "p-Seebeck",
    "p-powerfact",
    "pcond",
    "pkappa",
    "poisson",
    "raw_files",
    "reference",
    "search",
    "shear_modulus_gv",
    "slme",
    "spg",
    "spg_number",
    "spg_symbol",
    "spillage",
    "typ",
    "xml_data_link",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

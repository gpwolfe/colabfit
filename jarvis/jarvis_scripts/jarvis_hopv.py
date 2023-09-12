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
 'b3lyp_description',
 'b3lyp_gap',
 'b3lyp_homo',
 'b3lyp_lumo',
 'b3lyp_scharber_jsc',
 'b3lyp_scharber_pce',
 'b3lyp_scharber_voc',
 'bp86_description',
 'bp86_gap',
 'bp86_homo',
 'bp86_lumo',
 'bp86_scharber_jsc',
 'bp86_scharber_pce',
 'bp86_scharber_voc',
 'id',
 'm06_description',
 'm06_gap',
 'm06_homo',
 'm06_lumo',
 'm06_scharber_jsc',
 'm06_scharber_pce',
 'm06_scharber_voc',
 'pbe0_description',
 'pbe0_gap',
 'pbe0_homo',
 'pbe0_lumo',
 'pbe0_scharber_jsc',
 'pbe0_scharber_pce',
 'pbe0_scharber_voc']
"""

from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

# from colabfit.tools.property_definitions import potential_energy_pd


DATASET_FP = Path("jarvis_json_zips/")
GLOB = "hopv_15.json"
DS_NAME = "JARVIS_HOPV"
DS_DESC = (
    "The JARVIS_HOPV dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the Harvard organic photovoltaic (HOPV) dataset, collated "
    "experimental data from the literature. Quantum-chemical calculations are "
    "performed using five functionals: NP86, PBE0, B3LYP, M06-2X and basis sets. "
    "In addition to HOMO, LUMO and gap energies, calculations include open circuit "
    "potentials (Voc), short circuit current density (Jsc) and percent conversion "
    "efficiency (PCE). "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LINKS = [
    "https://doi.org/10.1038/sdata.2016.86",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/28814184",
]
AUTHORS = [
    "Steven A. Lopez",
    "Edward O. Pyzer-Knapp",
    "Gregor N. Simm",
    "Trevor Lutzow",
    "Kewei Li",
    "Laszlo R. Seress",
    "Johannes Hachmann",
    "Alán Aspuru-Guzik",
]
ELEMENTS = None


PROPERTY_MAP = {
    "band-gap": [
        {
            "energy": {"field": "pbe0_gap", "units": "eV"},
            "_metadata": {
                "software": {"value": "Q-CHEM 4.1.2"},
                "method": {"value": "DFT-PBE0"},
                "basis-set": {"value": "double-ζ def2-SV"},
            },
        },
        {
            "energy": {"field": "b3lyp_gap", "units": "eV"},
            "_metadata": {
                "software": {"value": "Q-CHEM 4.1.2"},
                "method": {"value": "DFT-B3LYP"},
                "basis-set": {"value": "double-ζ def2-SV"},
            },
        },
        {
            "energy": {"field": "bp86_gap", "units": "eV"},
            "_metadata": {
                "software": {"value": "Q-CHEM 4.1.2"},
                "method": {"value": "DFT-BP86"},
                "basis-set": {"value": "double-ζ def2-SV"},
            },
        },
        {
            "energy": {"field": "m06_gap", "units": "eV"},
            "_metadata": {
                "software": {"value": "Q-CHEM 4.1.2"},
                "method": {"value": "DFT-M06-2X"},
                "basis-set": {"value": "double-ζ def2-SV"},
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
    "b3lyp_description",
    # "b3lyp_gap",            <-- gap
    "b3lyp_homo",
    "b3lyp_lumo",
    "b3lyp_scharber_jsc",
    "b3lyp_scharber_pce",
    "b3lyp_scharber_voc",
    "bp86_description",
    # "bp86_gap",             <-- gap
    "bp86_homo",
    "bp86_lumo",
    "bp86_scharber_jsc",
    "bp86_scharber_pce",
    "bp86_scharber_voc",
    "id",
    "m06_description",
    # "m06_gap",              <-- gap
    "m06_homo",
    "m06_lumo",
    "m06_scharber_jsc",
    "m06_scharber_pce",
    "m06_scharber_voc",
    "pbe0_description",
    # "pbe0_gap",              <-- gap
    "pbe0_homo",
    "pbe0_lumo",
    "pbe0_scharber_jsc",
    "pbe0_scharber_pce",
    "pbe0_scharber_voc",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

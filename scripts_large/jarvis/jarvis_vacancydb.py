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

['bulk_atoms',
 'bulk_energy',
 'bulk_formula',
 'chem_pot',
 'defective_atoms',
 'defective_energy',
 'ef',
 'ff_vac',
 'id',
 'jid',
 'material_type',
 'symbol',
 'wycoff']
"""

from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

# from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path("jarvis_json2/")
GLOB = "vacancydb.json"
DS_NAME = "JARVIS_Vacancy_DB"
DS_DESC = (
    "The JARVIS_Vacancy_DB dataset is part of the joint automated repository "
    "for various integrated simulations (JARVIS) DFT database. This subset contains "
    "configurations sourced from the JARVIS-DFT-2D dataset, rerelaxed with Quantum "
    "ESPRESSO. JARVIS is a set of "
    "tools and datasets built to meet current materials design challenges."
)

LINKS = [
    "https://figshare.com/ndownloader/files/40750811",
    "https://jarvis.nist.gov/",
    "https://doi.org/10.1063/5.0135382",
    "https://doi.org/10.48550/arXiv.2205.08366",
]
AUTHORS = [
    "Kamal Choudhary",
    "Bobby G. Sumpter",
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
        bulk_atoms = row.pop("bulk_atoms")
        if bulk_atoms["cartesian"] is True:
            bulk_config = AtomicConfiguration(
                positions=bulk_atoms["coords"],
                symbols=bulk_atoms["elements"],
                cell=bulk_atoms["lattice_mat"],
            )
        else:
            bulk_config = AtomicConfiguration(
                scaled_positions=bulk_atoms["coords"],
                symbols=bulk_atoms["elements"],
                cell=bulk_atoms["lattice_mat"],
            )

        defect_atoms = row.pop("defective_atoms")
        defect_coords = defect_atoms["coords"]
        if type(defect_coords[0]) == float:
            defect_coords = [defect_coords]

        if defect_atoms["cartesian"] is True:
            defect_config = AtomicConfiguration(
                positions=defect_atoms["coords"],
                symbols=defect_atoms["elements"],
                cell=defect_atoms["lattice_mat"],
            )

        else:
            defect_config = AtomicConfiguration(
                scaled_positions=defect_coords,
                symbols=defect_atoms["elements"],
                cell=defect_atoms["lattice_mat"],
            )
        defect_config.info["energy"] = row.pop("defective_energy")
        bulk_config.info["energy"] = row.pop("bulk_energy")

        bulk_config.info["name"] = f"{fp.stem}_bulk_{i}"
        for key, val in row.items():
            if type(val) == str and val != "na" and len(val) > 0:
                bulk_config.info[key] = val
            elif type(val) == list and len(val) > 0 and any([x != "" for x in val]):
                bulk_config.info[key] = val
            elif type(val) == dict and all([v != "na" for v in val.values()]):
                bulk_config.info[key] = val
            elif type(val) == float or type(val) == int:
                bulk_config.info[key] = val
            else:
                pass

        defect_config.info["name"] = f"{fp.stem}_defective_{i}"
        for key, val in row.items():
            if type(val) == str and val != "na" and len(val) > 0:
                defect_config.info[key] = val
            elif type(val) == list and len(val) > 0 and any([x != "" for x in val]):
                defect_config.info[key] = val
            elif type(val) == dict and all([v != "na" for v in val.values()]):
                defect_config.info[key] = val
            elif type(val) == float or type(val) == int:
                defect_config.info[key] = val
            else:
                pass
        configs.append(bulk_config)
        configs.append(defect_config)
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
    css = [
        (
            "JARVIS-mlearn-Ni",
            "mlearn_Ni.*",
            "Ni configurations from JARVIS-mlearn dataset",
        ),
    ]
    cs_ids = []

    for name, reg, desc in css:
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            query={"names": {"$regex": reg}},
            name=name,
            description=desc,
            ds_id=ds_id,
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_do_ids,
        cs_ids=cs_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


CO_KEYS = [
    "bulk_atoms",
    "bulk_energy",
    "bulk_formula",
    "chem_pot",
    "defective_atoms",
    "defective_energy",
    "ef",
    "ff_vac",
    "id",
    "jid",
    "material_type",
    "symbol",
    "wycoff",
]
CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

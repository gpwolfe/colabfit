"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/phase_change_memory_materials")

SOFTWARE = "CASTEP"

PUBLICATION = "https://doi.org/10.1038/s41928-023-01030-x"
DATA_LINK = "https://doi.org/10.5281/zenodo.8208202"
LINKS = [
    "https://doi.org/10.5281/zenodo.8208202",
    "https://doi.org/10.1038/s41928-023-01030-x",
]
AUTHORS = ["Yuxing Zhou", "Wei Zhang", "Evan Ma", "Volker L. Deringer"]
DATASET_DESC = (
    "GST-GAP-22 contains configurations of phase-change materials on "
    "the quasi-binary GeTe-Sb2Te3 (GST) line of chemical compositions. "
    "Data was used for training a machine learning interatomic potential "
    "to simulate a range of germanium-antimony-tellurium compositions "
    "under realistic device conditions."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"field": "method"},
    "energy-cutoff": {"value": "600 eV"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "GPa"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "masses": {"field": "Masses"},
    "config-type": {"field": "config_type"},
    "sub-config": {"field": "sub_config"},
}

DSS = [
    [
        "GST_GAP_22_main",
        "main_GST-GAP-22_PBEsol.xyz",
        "The main training dataset for GST_GAP_22, "
        "calculated using the PBEsol functional. " + DATASET_DESC,
    ],
    [
        "GST_GAP_22_extended",
        "extended_GST-GAP-22_for_efield_PBEsol.xyz",
        "The extended training dataset for GST_GAP_22, calculated using the "
        "PBEsol functional. New configurations, simulated under external "
        "electric fields, were labelled with DFT and added to the original "
        "reference database " + DATASET_DESC,
    ],
    [
        "GST_GAP_22_refitted",
        "refitted_GST-GAP-22_PBE.xyz",
        "The training dataset for GST_GAP_22, "
        "recalculated using the PBE functional. " + DATASET_DESC,
    ],
]


def reader(fp):
    configs = read(fp, index=":")
    method = fp.stem.split("_")[-1]
    for config in configs:
        name = config.info.get("config_type")
        subconf = config.info.get("sub_config")
        if subconf and name != subconf:
            name += f"_{subconf}"
        config.info["name"] = f"{fp.stem.lower().replace('-','_')}_{name}"
        config.info["method"] = method
        stress = config.info.get("virial_stress")
        if stress is not None:
            config.info["stress"] = [
                [stress[0], stress[1], stress[2]],
                [stress[3], stress[4], stress[5]],
                [stress[6], stress[7], stress[8]],
            ]
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)
    for i, (ds_name, ds_glob, ds_desc) in enumerate(DSS):
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=ds_glob,
            generator=False,
        )

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
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=LINKS,
            description=ds_desc,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

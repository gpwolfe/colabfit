"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.5281/zenodo.4734035

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
energy
forces
Other properties added to metadata
----------------------------------
w_energy
w_forces
uncorrected energy
corrected energy per atom
dmin

File notes
----------
Per following link, it seems the most correct energy calculation to use
from this dataset will be the corrected energy, as the materials involved are
metals.
https://cms-lab.github.io/edu/AMM/FHI_aims_lab1.pdf

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import pandas as pd
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/cu_fhiaims_npjcm_2021"
)
DATASET_FP = Path().cwd().parent / "data/cu_fhiaims_npjcm_2021"
DATASET = "Cu_FHI-aims_NPJCM_2021"

SOFTWARE = "FHI-aims"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1038/s41524-021-00559-9"
DATA_LINK = "https://doi.org/10.5281/zenodo.4734035"
LINKS = [
    "https://doi.org/10.5281/zenodo.4734035",
    "https://doi.org/10.1038/s41524-021-00559-9",
]
AUTHORS = [
    "Yury Lysogorskiy",
    "Cas van der Oord",
    "Anton Bochkarev",
    "Sarath Menon",
    "Matteo Rinaldi",
    "Thomas Hammerschmidt",
    "Matous Mrovec",
    "Aidan Thompson",
    "Gábor Csányi",
    "Christoph Ortner",
    "Ralf Drautz",
]
DS_DESC = "Approximately 46,000 configurations of copper, including small and\
 bulk structures, surfaces, interfaces, point defects, and randomly modified\
 variants. Also includes structures with displaced or missing atoms."
ELEMENTS = ["Cu"]
GLOB_STR = "*.json"


def row_to_ase_atoms(row):
    symbols = row["_OCCUPATION"]
    pbc = row["pbc"]
    coordinates = row["_COORDINATES"]
    cell = row["cell"]

    if row["COORDINATES_TYPE"] == "relative":
        atoms = AtomicConfiguration(
            symbols=symbols, scaled_positions=coordinates, cell=cell, pbc=pbc
        )
    elif row["COORDINATES_TYPE"] == "absolute":
        atoms = AtomicConfiguration(symbols=symbols, positions=coordinates, pbc=pbc)
    else:
        raise ValueError("Unrecognized COORDINATES_TYPE:" + row["COORDINATES_TYPE"])
    atoms.info["energy"] = row["energy_corrected"]
    atoms.info["forces"] = row["forces"]
    atoms.info["energy_uncorrected"] = row["energy"]
    atoms.info["w_energy"] = row["w_energy"]
    atoms.info["w_forces"] = row["w_forces"]
    atoms.info["energy_corrected_per_atom"] = row["energy_corrected_per_atom"]
    atoms.info["dmin"] = row["dmin"]
    return atoms


def reader(filepath):
    df = pd.read_json(filepath, orient="records")
    configs = df.apply(row_to_ase_atoms, axis=1)
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
    }
    co_md = {
        "energy_uncorrected": {"field": "energy"},
        "w_energy": {"field": "w_energy"},
        "w_forces": {"field": "w_forces"},
        "energy_corrected_per_atom": {"field": "energy_corrected_per_atom"},
        "dmin": {"field": "dmin"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/angstrom"},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=co_md,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

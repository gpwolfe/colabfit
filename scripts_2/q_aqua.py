"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
xyz files were reformatted to ext-xyz files by Alexander. These are on
the ingest pod as "*data-reformatted.xyz"

small enough to run from ingest pod

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    # atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)

# DATASET_FP = Path(
#     "/persistent/colabfit_raw_data/new_raw_datasets_2.0/q-AQUA/4b_data-reformatted.xyz"
# )
DATASET_FP = Path("data/q-aqua")
DATASET_NAME = "q-AQUA"

SOFTWARE = "Molpro"
INFO_DICT = {
    "2b_data": {
        "method": "CCSD(T)/CBS",
        "basis": "aug-cc-pVTZ + aug-cc-pVQZ extrapolation",
        "name": "q-aqua_2-body",
    },
    "3b_data": {
        "method": "CCSD(T)-F12a",
        "basis": "aug-cc-pVTZ",
        "name": "q-aqua_3-body",
    },
    "4b_data": {
        "method": "CCSD(T)-F12",
        "basis": "heavy-aug-cc-pVTZ",
        "name": "q-aqua_4-body",
    },
}

PUBLICATION = "https://doi.org/10.1021/acs.jpclett.2c00966"
DATA_LINK = "https://github.com/jmbowma/q-AQUA"
LINKS = [
    "https://doi.org/10.1021/acs.jpclett.2c00966",
    "https://github.com/jmbowma/q-AQUA",
]
AUTHORS = [
    "Qi Yu",
    "Chen Qu",
    "Paul L. Houston",
    "Riccardo Conte",
    "Apurba Nandi",
    "Joel M. Bowman",
]
DATASET_DESC = (
    "The a-AQUA dataset was generated to address the need for a training set "
    "for a water PES that includes 2-body, 3-body and 4-body interactions calculated "
    "at the CCSD(T) level of theory. Structures were selected from the existing "
    "HBB2-pol and MB-pol datasets. "
    "For each water dimer structure, CCSD(T)/aug-cc-pVTZ calculations were performed "
    "with an additional 3s3p2d1f basis set; exponents equal to (0.9, 0.3, 0.1) for "
    "sp, (0.6, 0.2) for d, and 0.3 for f. This additional basis is placed at the "
    "center of mass (COM) of each dimer configuration. The basis set superposition "
    "error (BSSE) correction was determined with the counterpoise scheme. "
    "CCSD(T)/aug-cc-pVQZ calculations were then performed with the same additional "
    "basis set and BSSE correction. Final CCSD(T)/CBS energies were obtained by "
    "extrapolation over the CCSD(T)/aug-cc-pVTZ and CCSD(T)/aug-cc-pVQZ 2-b energies. "
    "All ab initio calculations were performed using Molpro package."
    "Trimer structures were calculated at CCSD(T)-F12a/aug-cc-pVTZ with BSSE "
    "correction. Four-body structure calculations were performed at CCSD(T)-F12 level."
)
ELEMENTS = [""]

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"field": "method"},
    "basis-set": {"field": "basis"},
}


PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


def tform(c):
    c.info["per-atom"] = False


def reader(fp):
    data = read(fp, index=":")
    n_body = fp.stem.split("-")[0]
    name = INFO_DICT[n_body]["name"]
    method = INFO_DICT[n_body]["method"]
    basis = INFO_DICT[n_body]["basis"]

    for i, config in enumerate(data):
        config.info["name"] = f"{name}_{i}"
        config.info["method"] = method
        config.info["basis"] = basis
    return data


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
    # client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        verbose=True,
        reader=reader,
        generator=False,
        glob_string="2b_data-reformatted.xyz",
    )

    configurations += load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        verbose=True,
        reader=reader,
        generator=False,
        glob_string="3b_data-reformatted.xyz",
    )

    configurations += load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        verbose=True,
        reader=reader,
        generator=False,
        glob_string="4b_data-reformatted.xyz",
    )

    client.insert_property_definition(potential_energy_pd)

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=PROPERTY_MAP,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # matches to data CO "name" field
    cs_regexes = (
        (
            "q-AQUA_2-body",
            "q-aqua_2-body",
            "2-body CCSD(T)/CBS interaction energies from q-AQUA",
        ),
        (
            "q-AQUA_3-body",
            "q-aqua_3-body",
            "3-body, BSSE-corrected CCSD(T)-F12a/aVTZ interaction energies from q-AQUA",
        ),
        (
            "q-AQUA_4-body",
            "q-aqua_4-body",
            "4-body CCSD(T)-F12/haTZ interaction energies from q-AQUA",
        ),
    )

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
    )


if __name__ == "__main__":
    main(sys.argv[1:])

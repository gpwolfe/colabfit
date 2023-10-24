"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
potential energy
atomic forces

File notes
----------
xyz header:
Lattice
Properties=species:S:1:pos:R:3:forces:R:3
energy=-206.92580857
pbc="F F F"

from publication SI
            Al12    Al24    Al48    Al0
k-points    5x5x2   5x3x2   3x3x2   5x3x2
"""

from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/a-AlOx/")
# DATASET_FP = Path().cwd().parent / "data/a-AlOx"  # comment out, local path
DS_NAME = "a-AlOx_JCP_2020"
DS_DESC = (
    "This dataset was used for the training of an MLIP for amorphous alumina (a-AlOx). "
    "Two configurations sets correspond to i) the actual training data and ii) "
    "additional reference data. "
    "Ab initio calculations were performed"
    "with the Vienna Ab initio Simulation Package. The projector"
    "augmented wave method was used to treat the atomic core electrons,"
    "and the Perdew-Burke-Ernzerhof functional within the generalized "
    "gradient approximation was used to describe the electron-electron "
    "interactions. The cutoff energy for the plane-wave basis set was "
    "set to 550 eV during the ab initio calculation. The obtained reference "
    "database includes the DFT energies of 41,203 structures. "
    "The supercell size of the AlOx reference structures varied from 24 to 132 atoms. "
    "K-point values are given for structures with: Al0, Al12, Al24, Al48 and Al192."
)
AUTHORS = ["Wenwen Li", "Yasunobu Ando", "Satoshi Watanabe"]
LINKS = [
    "https://doi.org/10.1063/5.0026289",
    "https://doi.org/10.24435/materialscloud:y1-zd",
]
GLOB = "*.xyzdat"

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "energy-cutoff": {"value": "550 eV"},
    "kpoints": {"field": "kpoint"},
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
}


def reader(fp):
    configs = read(fp, index=":", format="extxyz")
    # configs = read(fp, index="::100", format="extxyz") # local testing
    for i, config in enumerate(configs):
        symbols = str(config.symbols)
        config.info["name"] = f"{fp.stem}_{i}"
        if "Al12" in symbols:
            config.info["kpoint"] = "5x5x2"
        elif "Al24" in symbols:
            config.info["kpoint"] = "5x3x2"
        elif "Al48" in symbols:
            config.info["kpoint"] = "3x3x2"
        elif "Al" not in symbols:
            config.info["kpoint"] = "5x3x2"
        elif "Al192" in symbols:
            config.info["kpoint"] = "single gamma k-point"
        else:
            pass

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
        elements=["Al", "O"],
        verbose=True,
        reader=reader,
        generator=False,
        glob_string=GLOB,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    css = [
        (
            "train",
            "a-AlOx_training",
            "Structures used for training of neural network potential.",
        ),
        (
            "additional",
            "a-AlOx_reference",
            "Additional reference DFT calculations that author used for reference.",
        ),
    ]
    cs_ids = []
    for i, (reg, name, desc) in enumerate(css):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": reg}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        cs_ids=cs_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

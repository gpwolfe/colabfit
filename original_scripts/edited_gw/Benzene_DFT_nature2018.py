#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data
from ase.atoms import Atoms


DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets")
DATASET = "Benzene_DFT_NC2018"

LINKS = [
    "https://www.nature.com/articles/s41467-018-06169-2",
    "http://sgdml.org/",
]
AUTHORS = [
    "Stefan Chmiela",
    "Huziel E. Sauceda",
    "Klaus-Robert Müller",
    "Alexandre Tkatchenko",
]
DS_DESC = (
    "The data used for training the DFT models were created running ab initio "
    "MD in the NVT ensemble using the Nosé-Hoover thermostat at 500 K during "
    "a 200 ps simulation with a resolution of 0.5 fs. Forces and "
    "energies were computed using all-electrons at the generalized gradient "
    "approximation level of theory with the Perdew-Burke-Ernzerhof (PBE) "
    "exchange-correlation functional, treating van der Waals interactions with "
    "the Tkatchenko-Scheffler "
    "(TS) method. All calculations were performed with FHI-aims. The final "
    "training data was generated by subsampling the full trajectory under"
    " preservation of the Maxwell-Boltzmann distribution for the energies."
)


def tform(c):
    c.info["per-atom"] = False


# sGDML->checkout README
def reader_sGDML(filepath):
    with open(filepath, "r") as f:
        configs = []
        lines = f.readlines()
        while len(lines) > 0:
            symbols = []
            positions = []
            forces = []
            natoms = int(lines.pop(0))
            energy = float(lines.pop(0))
            for _ in range(natoms):
                line = lines.pop(0)
                symbol = line.split()[0]
                positions.append([float(p) for p in line.split()[1:4]])
                forces.append([float(f) for f in line.split()[4:]])
                symbol = symbol.lower().capitalize()
                symbols.append(symbol)
            config = Atoms(symbols=symbols, positions=positions)
            config.info["energy"] = energy
            config.info["forces"] = forces
            configs.append(config)
    for i, a in enumerate(configs):
        a.info["per-atom"] = False
        a.info["_name"] = f"{filepath.stem}_{i}"
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
    configurations = load_data(
        file_path=DATASET_FP / "sGDML",
        file_format="folder",
        name_field="_name",
        elements=["C", "H"],
        reader=reader_sGDML,
        glob_string="benzene2018_dft_FHI-aims.xyz",
        default_name="benzene2018_dft_FHI-aims",
        verbose=True,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "FHI-aims"},
                    "method": {"value": "DFT-PBE+TS"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/molAng"},
                "_metadata": {
                    "software": {"value": "FHI-aims"},
                    "method": {"value": "DFT-PBE+TS"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

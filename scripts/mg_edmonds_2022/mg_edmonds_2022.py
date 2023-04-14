"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.17617/3.A3MB7Z
File address:
https://edmond.mpdl.mpg.de/file.xhtml?fileId=194528&version=1.0

Unzip files:
tar -zxf structures_packed.tar.gz -C <project_directory>/scripts/mg_edmonds_2022

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Properties:
forces

Other properties added to metadata:
total energy
stress (a stress tensor of length 6, which does not fit
the 3x3 of our Cauchy stress property definition)

File notes
----------
"""
from argparse import ArgumentParser
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
)
import h5py
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("structures_packed/")


def reader(filepath: Path):
    atoms = []
    with h5py.File(filepath) as f:
        file_key = list(f.keys())[0]

        # Do not need indexing (i.e., one value per configuration)
        cells = np.array(f[file_key]["structures"]["chunk_arrays"]["cell"])
        pbcs = list(f[file_key]["structures"]["chunk_arrays"]["pbc"])
        names = [
            id.decode()
            for id in f[file_key]["structures"]["chunk_arrays"]["identifier"]
        ]
        stress = np.array(f[file_key]["structures"]["chunk_arrays"]["stress"])
        energy = np.array(f[file_key]["structures"]["chunk_arrays"]["energy"])
        start_index = np.array(
            f[file_key]["structures"]["chunk_arrays"]["start_index"]
        )
        #   num_atoms = np.array(
        #       f[file_key]["structures"]["chunk_arrays"]["length"]
        #   )

        # Need indexing (multiple rows per configuration)
        forces = np.array(
            f[file_key]["structures"]["element_arrays"]["forces"]
        )
        coords = np.array(
            f[file_key]["structures"]["element_arrays"]["positions"]
        )
        element = np.array(["Mg" for x in coords])

    # Remove first index to avoid blank array
    start_index = start_index[1:]

    forces = np.split(forces, start_index)
    coords = np.split(coords, start_index)
    element = np.split(element, start_index)

    for coords, element, pbcs, cells, stress, energy, forces, names in zip(
        coords, element, pbcs, cells, stress, energy, forces, names
    ):
        atom = Atoms(positions=coords, symbols=element, pbc=pbcs, cell=cells)
        atom.info["stress"] = stress
        atom.info["energy"] = energy
        atom.info["forces"] = forces
        atom.info["name"] = f"{file_key}_{names}"
        atoms.append(atom)
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Mg"],
        reader=reader,
        glob_string="*.h5",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    metadata = {
        "software": {"value": "MLIP, VASP"},
        "method": {"value": "DFT"},
        "total-energy": {"field": "energy"},
        # this is a stress tensor of size 6, not 9 or 3x3
        "stress": {"field": "stress"},
    }

    property_map = {
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "Unknown"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            "mg_edmonds_2022_CellMin",
            "CellMin*",
            "Configurations from mg_edmonds_2022 in CellMin dataset",
        ],
        [
            "mg_edmonds_2022_Everything",
            "Everything_*",
            "Configurations from mg_edmonds_2022 in Everything dataset \
               (defined by dataset author)",
        ],
        [
            "mg_edmonds_2022_EverythingNoShear",
            "EverythingNoShear*",
            "Configurations from mg_edmonds_2022 in EverythingNoShear dataset",
        ],
        [
            "mg_edmonds_2022_Hydro",
            "Hydro*",
            "Configurations from mg_edmonds_2022 in Hydro dataset",
        ],
        [
            "mg_edmonds_2022_IntMin",
            "IntMin*",
            "Configurations from mg_edmonds_2022 in IntMin dataset",
        ],
        [
            "mg_edmonds_2022_RandSPG",
            "RandSPG*",
            "Configurations from mg_edmonds_2022 in RandSPG dataset",
        ],
        [
            "mg_edmonds_2022_Rattle",
            "Rattle*",
            "Configurations from mg_edmonds_2022 in Rattle dataset",
        ],
        [
            "mg_edmonds_2022_Shear",
            "Shear*",
            "Configurations from mg_edmonds_2022 in Shear dataset",
        ],
        [
            "mg_edmonds_2022_VolMin",
            "VolMin*",
            "Configurations from mg_edmonds_2022 in VolMin dataset",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        cs_id = client.insert_configuration_set(
            co_ids, description=desc, name=name
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name="MG_edmonds_2022",
        authors=["Marvin Poul"],
        links=[
            "https://github.com/eisenforschung/magnesium-mtp-training-data",
            "https://doi.org/10.17617/3.A3MB7Z",
            "https://arxiv.org/abs/2207.04009",
        ],
        description="16748 configurations of magnesium with gathered energy, "
        "stress and forces at the DFT level of theory.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

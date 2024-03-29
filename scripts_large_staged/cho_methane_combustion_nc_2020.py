"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.6084/m9.figshare.12973055.v3

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
energy
forces
virial

Other properties added to metadata
----------------------------------

File notes
----------
"""
from argparse import ArgumentParser
from colabfit import ATOMS_NAME_FIELD, ATOMS_LABELS_FIELD
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys
from tqdm import tqdm

BATCH_SIZE = 2

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts_large/large_scripts_data/"
    "cho_methane_combustion_nc_2020"
)
DATASET = "CHO-methane-combustion-NC-2020"

SOFTWARE = "Gaussian 16"
METHODS = "NVT-QM-MN15"

PUBLICATION = "https://doi.org/10.1038/s41467-020-19497-z"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.12973055.v3"
LINKS = [
    "https://doi.org/10.6084/m9.figshare.12973055.v3",
    "https://doi.org/10.1038/s41467-020-19497-z",
]
AUTHORS = [
    "Jinzhe Zeng",
    "Liqun Cao",
    "Mingyuan Xu",
    "Tong Zhu",
    "John Z.H. Zhang",
]
DS_DESC = (
    "Configurations of simulated methane combustion used to develop "
    "a potential energy surface neural network using DeePMD."
)
ELEMENTS = ["C", "H", "O"]
GLOB_STR = "box.npy"


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    a_types = []
    elem_map = dict()
    if "type_map.raw" in [x.name for x in (filepath.parents[1].glob("*.raw"))]:
        with open(filepath.parents[1] / "type_map.raw", "r") as f:
            types = [x.strip() for x in f.readlines()]
            for i, t in enumerate(types):
                elem_map[i] = t
        with open(filepath.parents[1] / "type.raw", "r") as f:
            a_types = f.readlines()
            if len(a_types) == 1:
                a_types = [int(x.strip()) for x in a_types[0].split()]
            else:
                a_types = [int(x.strip()) for x in a_types]
    elif "type_map.raw" in [x.name for x in (filepath.parent.glob("*.raw"))]:
        with open(filepath.parent / "type_map.raw", "r") as f:
            types = [x.strip() for x in f.readlines()]
            for i, t in enumerate(types):
                elem_map[i] = t
        with open(filepath.parent / "type.raw", "r") as f:
            a_types = f.readlines()
            if len(a_types) == 1:
                a_types = [int(x.strip()) for x in a_types[0].split()]
            else:
                a_types = [int(x.strip()) for x in a_types]
    else:
        pass
    symbols = list(map(lambda x: elem_map.get(x), a_types))
    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = props["force"].shape[1] // 3
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    virial = props.get("virial")
    if virial is not None:
        props["virial"] = virial.reshape(num_configs, 3, 3)
    props["symbols"] = symbols
    return props


def reader(filepath):
    props = assemble_props(filepath)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["forces"][i]
        virial = props.get("virial")
        if virial is not None:
            c.info["virial"] = virial[i]
        # if energy is not None:
        c.info["energy"] = float(energy[i])

        c.info["name"] = f"{filepath.parts[-3]}_{filepath.parts[-2]}_{i}"
    return configs


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
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "basis-set": {"value": "6-31G**"},
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
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }

    name_field = "name"
    labels_field = "labels"
    ds_id = generate_ds_id()
    ai = 0
    ids = []
    fps = list(DATASET_FP.rglob(GLOB_STR))
    n_batches = len(fps) // BATCH_SIZE
    leftover = len(fps) % BATCH_SIZE
    indices = [((b * BATCH_SIZE, (b + 1) * BATCH_SIZE)) for b in range(n_batches)]
    if leftover:
        indices.append((BATCH_SIZE * n_batches, len(fps)))
    for batch in tqdm(indices):
        configurations = []
        beg, end = batch
        for fi, fpath in enumerate(fps[beg:end]):
            new = reader(fpath)

            for atoms in new:
                a_elems = set(atoms.get_chemical_symbols())
                if not a_elems.issubset(ELEMENTS):
                    raise RuntimeError(
                        "Image {} elements {} is not a subset of {}.".format(
                            ai, a_elems, ELEMENTS
                        )
                    )
                else:
                    if name_field in atoms.info:
                        name = []
                        name.append(atoms.info[name_field])
                        atoms.info[ATOMS_NAME_FIELD] = name
                    else:
                        raise RuntimeError(
                            f"Field {name_field} not in atoms.info for index "
                            f"{ai}. Set `name_field=None` "
                            "to use `default_name`."
                        )

                if labels_field not in atoms.info:
                    atoms.info[ATOMS_LABELS_FIELD] = set()
                else:
                    atoms.info[ATOMS_LABELS_FIELD] = set(atoms.info[labels_field])
                ai += 1
                configurations.append(atoms)

        ids.extend(
            list(
                client.insert_data(
                    configurations,
                    ds_id=ds_id,
                    property_map=property_map,
                    generator=False,
                    verbose=False,
                )
            )
        )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

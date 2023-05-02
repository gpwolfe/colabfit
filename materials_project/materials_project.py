from argparse import ArgumentParser
from ase.io import read
from colabfit import ATOMS_LABELS_FIELD, ATOMS_NAME_FIELD
from colabfit.tools.converters import AtomicConfiguration
from colabfit.tools.database import MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
)
import numpy as np
from pathlib import Path
import sys
from tqdm import tqdm

BATCH_SIZE = 1

DATASET_FP = Path("mat_proj_xyz_files2")
DATASET = "Materials Project"

AUTHORS = [
    "A. Jain",
    "S.P. Ong",
    "G. Hautier",
    "W. Chen",
    "W.D. Richards",
    "S. Dacek",
    "S. Cholia",
    "D. Gunter",
    "D. Skinner",
    "G. Ceder",
    "K.A. Persson",
]
DS_DESC = (
    "Configurations from the Materials Project database:"
    " an online resource with the goal of computing properties of all"
    " inorganic materials."
)

GLOB_STR = "*.xyz"

ELEMENTS = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
]


def reconstruct_nested(info: dict, superkey: str):
    """
    Create dicts, nested if necessary, for configuration info from xyz headers.

    Incar, outcar and output information will be gathered from Mat. Proj. xyz file
    headers, checked for incorrect values (where info value contains another key-value
    pair), and gathered into a single, nested (if necessary) dictionary, and returned.
    """
    in_out_car = dict()
    for key, val in info.items():
        # Some values in header are blank, so ase.io.read returns key1 = "key2=val2"
        if type(val) == str and "=" in val:
            # print(val)
            key, val = val.split("=")[-2:]
            if val != "F":
                try:
                    val = float(val)
                except ValueError:
                    pass
        if superkey in key:
            # pymongo/MongoDB throws error regarding use of numpy objects:
            # bson.errors.InvalidDocument: cannot encode object: 100, \
            # of type: <class 'numpy.int64'>
            if type(val) == np.ndarray:
                val = val.tolist()
            if type(val) == np.int64:
                val = int(val)
            outkey = key.split("-")
            if len(outkey) == 2:
                in_out_car[outkey[1]] = val
            elif len(outkey) == 3:
                if not in_out_car.get(outkey[1]):
                    in_out_car[outkey[1]] = {outkey[2]: val}
                else:
                    in_out_car[outkey[1]][outkey[2]] = val
    return in_out_car


def reader(file_path):
    atom_configs = []
    configs = read(file_path, index=":")
    for i, config in enumerate(configs):
        info = dict()
        info["outcar"] = reconstruct_nested(config.info, "outcar")
        info["output"] = reconstruct_nested(config.info, "output")
        info["incar"] = reconstruct_nested(config.info, "incar")
        for key, val in config.info.items():
            if not any([match in key for match in ["outcar", "incar", "output"]]):
                if type(val) == str and "=" in val:
                    key, val = val.split("=")[-2:]
                    print(key, val)
                    if val != "F":
                        try:
                            val = float(val)
                        except ValueError:
                            pass
                info[key] = val
        atoms = AtomicConfiguration(
            numbers=config.numbers,
            positions=config.positions,
            cell=config.cell,
            pbc=config.pbc,
        )
        atoms.info = info
        atom_configs.append(atoms)
    return atom_configs


def main(ip, db_name, nprocs):
    client = MongoDatabase(db_name, nprocs=nprocs, uri=f"mongodb://{ip}:27017")
    for pd in [
        atomic_forces_pd,
        cauchy_stress_pd,
        free_energy_pd,
    ]:
        client.insert_property_definition(pd)

    metadata = {
        "software": {"value": "VASP"},
        "method": {"field": "calc_type"},
    }
    # excluded keys are included under other names or in property_map
    exclude = {"calc_type", "e_fr_energy", "forces", "stress", "material_id"}
    co_md_map = {
        "material-id": {"field": "material_id"},
        "internal_energy": {"field": "e_0_energy"},
    }
    co_md_map.update({k: {"field": k} for k in KEYS if k not in exclude})
    property_map = {
        "free-energy": [
            {
                "energy": {"field": "e_fr_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }

    name_field = "name"
    labels_field = "labels"
    ai = 0
    ids = []
    fps = sorted(list(DATASET_FP.rglob(GLOB_STR)))[:10]
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
                    co_md_map=co_md_map,
                    property_map=property_map,
                    generator=False,
                    verbose=False,
                )
            )
        )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=[
            "https://materialsproject.org/",
        ],
        description=DS_DESC,
        verbose=True,
    )


KEYS = [
    "calc_type",
    "e_0_energy",
    "e_fr_energy",
    "e_wo_entrp",
    "incar",
    "material_id",
    "name",
    "outcar",
    "output",
    "stress",
    "task_id",
]

if __name__ == "__main__":
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
    args = parser.parse_args(sys.argv[1:])

    ip = args.ip
    nprocs = args.nprocs
    db_name = args.db_name
    main(ip, db_name, nprocs)

    # files = sorted(list(DATASET_FP.glob("*.xyz")))
    # print(files)
    # # Import by batch, with first batch returning dataset-id
    # n_batches = len(files) // batch_size
    # batch_1 = files[:batch_size]
    # dataset_id = main(ip, db_name, nprocs, batch_1, None, None)

    # for n in range(1, n_batches):
    #     batch_n = files[batch_size * n : batch_size * (n + 1)]
    #     dataset_id = main(
    #         ip,
    #         db_name,
    #         nprocs,
    #         batch_n,
    #         dataset_id=dataset_id,
    #     )
    #     print(f"Dataset: {dataset_id}")

    # if len(files) % batch_size and len(files) > batch_size:
    #     batch_n = files[batch_size * n_batches :]
    #     dataset_id = main(ip, db_name, nprocs, batch_n, dataset_id)

from ase import Atoms
from colabfit.tools.converters import AtomicConfiguration
from collections import defaultdict
import numpy as np
from pathlib import Path

def read_npz(filepath):
    data = defaultdict(list)
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
    return data

# For assembling data in npy format scattered into a single category 
# with a type.raw file in parent directory
# This is the format associated with DeePMD models/datasets
ELEM_KEY = {1: "H", 2: "O"}
def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    type_path = list(filepath.parents[1].glob("type.raw"))[0]
    # Use below if multiple configuration types with different elements can be
    # sorted by parts of the filepath
    # for key in ELEM_KEY:
    #     if key in type_path.parts[-7:]:
    #         elem_key = ELEM_KEY[key]

    with open(type_path, "r") as f:
        nums = f.read().rstrip().split(" ")
        # If multiple config types and using "elem_key" above, change below
        props["symbols"] = [ELEM_KEY[int(num)] for num in nums]

    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = len(props["symbols"])
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    return props


# Above used with below:

def reader(filepath):
    props = assemble_props(filepath)
    print(filepath)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["forces"][i]
        # if energy is not None:
        c.info["energy"] = float(energy[i])
        c.info[
            "name"
        ] = f"{filepath.parts[-3]}_{filepath.parts[-5]}_{filepath.parts[-3]}_{i}"
    return configs


def assemble_npy_properties(filepath: Path):
    prop_path = filepath.parent.glob('*.npy')
    props = {}
    for p in prop_path:
        key = p.stem
        props[key] = np.load(p)
    return props


def basic_npz_reader(file):
    """This is an example for compressed numpy files (.npz)"""
    atoms = []
    with np.load(file) as npz:
        npz = np.load(file)
        for coords, energy, forces, md17_index in zip(
            npz["coords"],
            npz["energies"],
            npz["forces"],
            npz["old_indices"],
        ):
            atoms.append(
                Atoms(
                    numbers=npz["nuclear_charges"],
                    positions=coords,
                    info={
                        "name": file.stem,
                        "energy": energy,
                        "forces": forces,
                        "md17_index": md17_index,
                    },
                )
            )
    return atoms


def read_np(filepath: str, props: dict):

    """
    filepath: path to parent directory of numpy files
    props: dictionary containing keys equal to the keys outlined below and
        values equal to the equivalent numpy keys given by <filename>.files
    props = {
        'name': <name>,
        'coords': <key of coordinates>,
        'energy': <key of potential energy>,
        'forces': <key of forces>,
        'cell': <key of cell/lattice>,
        'pbc': <PBC True or False (set by user)>,
        'numbers': <key for atomic numbers>,
        'elements': <key for atomic elements
    }
    """
    file = Path(filepath)
    data = np.load(file)
    atoms = []

    file_props = {key: data[val] for key, val in props}
    if "elements" and "numbers" in file_props.keys():
        del file_props["numbers"]

        atoms.append(
            Atoms(
                numbers=file_props.get("numbers"),
                elements=file_props.get("elements"),
                positions=file_props.get("coords"),
                cell=file_props.get("cell", [0, 0, 0]),
                pbc=file_props.get("pbc", False),
                info={
                    "name": file_props.get("name", file.stem),
                    "potential_energy": file_props.get("energy"),
                    "cauchy_stress": file_props.get("stress"),
                    "nuclear_gradients": file_props.get("gradient"),
                    "partial_charges": file_props.get("charges"),
                },
            )
        )
    return atoms


def assemble_np(fp_dict, props: dict):
    """
    fp_dict: dictionary with filepaths as keys and the target property
        (as defined by the keys in props below) as value
    props: dictionary containing keys equal to the keys outlined below and
        values equal to the equivalent numpy keys given by <filename>.files
    props = {
        'name': <name>,
        'coords': <key of coordinates>,
        'energy': <key of potential energy>,
        'forces': <key of forces>,
        'cell': <key of cell/lattice>,
        'pbc': <PBC True or False (set by user)>,
        'numbers': <key for atomic numbers>,
        'elements': <key for atomic elements
    }
    """
    file_props = {}

    for fp, val in fp_dict.items():
        data = np.load(fp)
        # in case the file only contains, for instance, a single float value
        if not props.get(val):
            file_props[val] = data
        else:
            file_props[val] = data[props[val]]
    return file_props

def insert_configuration_set(client, names, res, descs):


cs_regexes = [
    [
        "All_H2/Pt(III)",
        "*",
        "All configurations from H/Pt(III)",
    ],
    [
        "H2_H2/Pt(III)",
        "H2*",
        "H2 configurations from H/Pt(III)",
    ],
    [
        "Pt-bulk_H2/Pt(III)",
        "Pt-bulk*",
        "Pt-bulk configurations from H/Pt(III)",
    ],
    [
        "Pt-surface_H2/Pt(III)",
        "Pt-surface*",
        "Pt-surface configurations from H/Pt(III)",
    ],
    [
        "PtH_H2/Pt(III)",
        "PtH*",
        "PtH configurations from H/Pt(III)",
    ],
]

cs_ids = []

for i, (name, regex, desc) in enumerate(cs_regexes):
    try:
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()
    except OperationFailure:
        print(f"No match for regex: {regex}")
        continue

    print(
        f"Configuration set {i}",
        f"({name}):".rjust(25),
        f"{len(co_ids)}".rjust(7),
    )

    if len(co_ids) == 0:
        pass
    else:    
        cs_id    = client.insert_configuration_set(
            co_ids, description=desc, name=name
        )

        cs_ids.append(cs_id)
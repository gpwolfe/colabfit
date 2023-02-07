from ase import Atoms
import numpy as np
from pathlib import Path

# import shutil as sh


def basic_npz_reader(file):
    """This reader works for compressed numpy files (.npz)"""
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

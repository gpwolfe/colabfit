"""From dataset authors"""

import pandas as pd
import numpy as np
import ase.io
from geom import create_geom
import h5py

LATTICE_CONSTANT = 2.46  # graphene lattice constant


def make_hdf(df, hdf_name="qmc.hdf", ntiling=3):
    """
    Creates HDF file with data from the specified `csv_name`
    Coordinates `coords` are generated from the supercell `(ntiling, ntiling, 1)`
    """
    data = {"coords": [], "latvecs": [], "atomic_numbers": [], "ntiling": []}

    for i, row in df.iterrows():
        registry = row["registry"]
        d = row["distance"]
        basis = row["basis"].upper()
        energy = row["energy"]
        energy_err = row["energy_err"]

        atoms = create_geom(registry, d, LATTICE_CONSTANT, h=40, n=1, basis=basis)

        atoms = atoms.repeat((ntiling, ntiling, 1))
        coords = atoms.get_positions()
        latvecs = atoms.get_cell()
        atomic_numbers = atoms.get_atomic_numbers()

        data["coords"].append(coords)
        data["latvecs"].append(latvecs)
        data["atomic_numbers"].append(atomic_numbers)
        data["ntiling"].append(ntiling)

    with h5py.File(hdf_name, "w") as hdf:
        for k, v in data.items():
            v = np.array(v)
            hdf.create_dataset(k, data=v, dtype=v.dtype)
        hdf.create_dataset("energy", data=df["energy"])
        hdf.create_dataset("energy_err", data=df["energy_err"])
        hdf.create_dataset("registry", data=df["registry"])
        hdf.create_dataset("distance", data=df["distance"])


def read_hdf(hdf_name):
    with h5py.File(hdf_name) as hdf:
        latvecs = hdf["latvecs"][...]
        coords = hdf["coords"][...]
        atomic_numbers = hdf["atomic_numbers"][...]
        energy = hdf["energy"][...]
        energy_err = hdf["energy_err"][...]

    # test extracting only the first configuration
    print(energy[0], energy_err[0])
    print(atomic_numbers[0])
    print(coords[0])
    atoms = ase.atoms.Atoms(
        numbers=atomic_numbers[0], positions=coords[0], pbc=(1, 1, 0)
    )
    atoms.write("test_read.xyz")


if __name__ == "__main__":
    df = pd.read_csv("all.csv")
    method = ["DFT-D2", "DFT-D3", "QMC"]
    for m in method:
        print(f"converting {m} data to HDF5")
        df_method = df.loc[df["method"] == m, :]
        for j, row in df_method.iterrows():
            make_hdf(df_method, hdf_name=f"{m}_BLG_hBN_structures.hdf", ntiling=3)

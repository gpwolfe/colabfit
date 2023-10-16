"""
author:



Properties
----------
potential energy
atomic forces

Other properties added to metadata
----------------------------------
dipole
quadrupole
octupole
volume

File notes
----------


methods, software (where stated) and basis sets as follows:
"hf_dz.energy": meth("HF",  "cc-pVDZ"),
"hf_qz.energy": meth("HF", "cc-pVQZ"),
"hf_tz.energy": meth("HF", "cc-pVTZ"),
# "mp2_dz.corr_energy": meth("MP2", "cc-pVDZ"),
# "mp2_qz.corr_energy": meth("MP2", "cc-pVQZ"),
# "mp2_tz.corr_energy": meth("MP2", "cc-pVTZ"),
# "npno_ccsd(t)_dz.corr_energy": meth("NPNO-CCSD(T)", "cc-pVDZ"),
# "npno_ccsd(t)_tz.corr_energy": meth("NPNO-CCSD(T)", "cc-pVTZ"),
# "tpno_ccsd(t)_dz.corr_energy": meth("TPNO-CCSD(T)", "cc-pVDZ"),
# "wb97x_dz.cm5_charges": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_dz.dipole": meth("wB97x", "Gaussian 09","6-31G*"),
"wb97x_dz.energy": meth("wB97x", "Gaussian 09","6-31G*"),
"wb97x_dz.forces": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_dz.hirshfeld_charges": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_dz.quadrupole": meth("wB97x", "Gaussian 09","6-31G*"),
# "wb97x_tz.dipole": meth("wB97x","ORCA","def2-TZVPP"),
"wb97x_tz.energy": meth("wB97x", "ORCA","def2-TZVPP"),
"wb97x_tz.forces": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_charges": meth("wB97x","ORCA", "def2-TZVPP"),
# "wb97x_tz.mbis_dipoles": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_octupoles": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_quadrupoles": meth("wB97x", "ORCA","def2-TZVPP"),
# "wb97x_tz.mbis_volumes": meth("wB97x", "ORCA","def2-TZVPP"),
"ccsd(t)_cbs.energy": meth("CCSD(T)*", "ORCA","CBS"),
# "wb97x_tz.quadrupole": meth("wB97x", "ORCA","def2-TZVPP")


Units for props are described here:
https://www.nature.com/articles/s41597-020-0473-z/tables/2
"""
from argparse import ArgumentParser

import h5py
import json
from multiprocessing.pool import Pool
from pathlib import Path

import numpy as np

from colabfit.tools.configuration import AtomicConfiguration

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/ani1x/")  # HSRN
DATASET_FP = Path("data/ani1x")  # local and Greene


def numpy_array_to_list(arr):
    if isinstance(arr, np.ndarray):
        if arr.ndim == 0:
            return arr.item()
        if arr.ndim == 1:
            return arr.tolist()
        return [numpy_array_to_list(subarray) for subarray in arr]
    return arr


def read_h5(h5filename):
    """
    Inspired by https://github.com/aiqm/ANI1x_datasets/tree/master
    Iterate over buckets of data in ANI HDF5 file.
    Yields dicts with atomic numbers (shape [Na,]) coordinated (shape [Nc, Na, 3])
    and other available properties specified by `keys` list, w/o NaN values.
    """

    with h5py.File(h5filename, "r") as f:
        keys = set(list(f.values())[0].keys())
        keys.discard("atomic_numbers")
        keys.discard("coordinates")
        for grp in f.values():
            data = {k: list(grp[k]) for k in keys}
            a_nums = list(grp["atomic_numbers"])
            for i, coords in enumerate(grp["coordinates"]):
                row = {k: data[k][i] for k in keys if (~np.isnan(data[k][i]).any())}
                row["coords"] = numpy_array_to_list(coords)
                row["a_nums"] = a_nums
                yield row


def reader(filepath: Path):
    for i, row in enumerate(read_h5(filepath)):
        config = AtomicConfiguration(
            positions=row.pop("coords"), numbers=row.pop("a_nums")
        )
        config.info = {key: val for key, val in row.items()}
        config.info["name"] = f"{filepath.stem}_{i}"
        config = config.todict()
        for key, val in config.items():
            if isinstance(val, np.ndarray):
                config[key] = numpy_array_to_list(val)
        for key, val in config["info"].items():
            if isinstance(val, np.ndarray):
                config["info"][key] = numpy_array_to_list(val)
        yield config


def main():
    with open("ani1x_configs.json", "w") as out_f:
        rdr = reader(Path("data/ani1x/ani1x-release.h5"))
        for i, config in enumerate(rdr):
            if i > 10000:
                break
            json.dump(config, out_f, separators=(",", ":"))
            out_f.write("\n")


if __name__ == "__main__":
    # main(sys.argv[1:])
    main()

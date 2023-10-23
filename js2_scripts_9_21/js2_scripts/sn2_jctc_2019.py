"""
author:

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.atoms import Atoms
import numpy as np
from tqdm import tqdm

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    # potential_energy_pd,
)


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets/SN2_UnkeOliverMeuwly/"
)
DATASET_NAME = "SN2_JCTC_2019"

SOFTWARE = ""
METHODS = ""
LINKS = [
    "https://doi.org/10.1021/acs.jctc.9b00181",
    "https://doi.org/10.5281/zenodo.2605341",
]
AUTHORS = ["Oliver T. Unke", "Markus Meuwly"]
DATASET_DESC = (
    "The SN2 dataset was generated as a partner benchmark dataset, "
    "along with the 'solvated protein fragments' dataset, for measuring the "
    "performance of machine learning models, in particular PhysNet, "
    "at describing chemical reactions, long-range interactions, "
    "and condensed phase systems. SN2 probes chemical reactions of "
    "methyl halides with halide anions, i.e. X- + CH3Y -> CH3X +  Y-, and "
    "contains structures, "
    "for all possible combinations of X,Y = F, Cl, Br, I. The dataset also includes "
    "various structures for several smaller molecules that can be formed in "
    "fragmentation reactions, such as CH3X, HX, CHX or CH2X- as well as geometries "
    "for H2, CH2, CH3+ and XY interhalogen compounds. In total, the dataset provides "
    "reference energies, forces, and dipole moments for 452709 structures"
    "calculated at the DSD-BLYP-D3(BJ)/def2-TZVP level of theory using ORCA 4.0.1."
)
ELEMENTS = ["C", "F", "Cl", "Br", "H", "I"]
GLOB_STR = "*.npz"


def reader_SN2(p):
    atoms = []
    a = np.load(p)
    na = a["N"]
    z = a["Z"]
    e = a["E"]
    r = a["R"]
    f = a["F"]
    d = a["D"]
    q = a["Q"]
    for i in tqdm(range(len(na))):
        n = na[i]
        atom = Atoms(numbers=z[i, :n], positions=r[i, :n, :])
        atom.info["energy"] = e[i]
        atom.arrays["forces"] = f[i, :n, :]
        # print (f[i,:n,:])
        atom.info["dipole_moment"] = d[i]
        atom.info["charge"] = q[i]
        atoms.append(atom)
    return atoms


def tform(c):
    c.info["per-atom"] = False


property_map = {
    #    'potential-energy': [{
    #        'energy':   {'field': 'energy',  'units': 'eV'},
    #        'per-atom': {'field': 'per-atom', 'units': None},
    #        '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    #    'cauchy-stress': [{
    #    'stress':   {'field': 'virial',  'units': 'GPa'},
    #                '_metadata': {
    #            'software': {'value':'VASP'},
    #        }
    #    }]
    "atomization-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    "dipole": [
        {
            "dipole": {"field": "dipole_moment", "units": "e*Ang"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
    "charge": [
        {
            "charge": {"field": "charge", "units": "e"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1 code"},
                "method": {"value": "DSD-BLYP-D3(BJ)/def2-TZVP"},
            },
        }
    ],
}


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
    client.insert_property_definition(atomic_forces_pd)
    # client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    # client.insert_property_definition(atomization_property_definition)
    # client.insert_property_definition(dipole_property_definition)
    # client.insert_property_definition(charge_property_definition)
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field=None,
        elements=ELEMENTS,
        default_name="SN2",
        reader=reader_SN2,
        glob_string=GLOB_STR,
        verbose=True,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            ds_id=ds_id,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

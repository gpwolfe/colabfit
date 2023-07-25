"""
Get proper author names
check file names and configuration types/names
check for config-md

"""

from argparse import ArgumentParser
from pathlib import Path

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd

from ase import Atoms
import numpy as np
from tqdm import tqdm

DS_NAME = "WS22"
AUTHORS = ["Pinheiro Jr", "M., Zhang", "S., Dral", "P. O.", "Barbatti, M."]
LINKS = [
    "https://www.nature.com/articles/s41597-023-01998-3#code-availability",
    "https://zenodo.org/record/7032334#.ZEDJes7MJEY",
]
DATASET_FP = Path("/large_data/new_raw_datasets_2.0/WS22_database")
DS_DESC = (
    "The WS22 database combines Wigner sampling with geometry interpolation to generate"
    " 1.18 million molecular geometries equally distributed into 10 independent "
    "datasets of flexible organic molecules with varying sizes and chemical "
    "complexity. In addition to the potential energy and forces required to construct "
    "potential energy surfaces, the WS22 database provides several other quantum "
    "chemical properties, all obtained via single-point calculations for each "
    "molecular geometry. All quantum chemical calculations were performed with the "
    "Gaussian09 program."
)


def reader_ws22(p):
    atoms = []
    a = np.load(p)
    # na=a['N']
    z = a["Z"]
    e = a["E"]
    r = a["R"]
    f = a["F"]
    hl = a["HL"]
    d = a["DP"]
    # q=a['nuclear_charges']
    # for i in tqdm(range(len(na))):  #need to change it
    for i in tqdm(1200):
        # n=na[i]
        # atom = Atoms(numbers=z[i, :], positions=r[i, :n, :])
        atom = Atoms(numbers=z, positions=r[i])
        # atom.info['energy']=e[i]
        atom.info["energy"] = float(e[i])
        atom.arrays["forces"] = f[i]
        atom.info["dipole_moment"] = d[i]
        atom.info["homolumo"] = hl[i]
        # atom.info['charge']=float(q[i])
        # print(atom.info['charge'])
        atoms.append(atom)
        # print(type (atom.info['charge']))
    return atoms


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "kcal/mol"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1"},
                "method": {"value": "PBE0/6-311G*"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "kcal/mol/A"},
            "_metadata": {
                "software": {"value": "ORCA 4.0.1"},
                "method": {"value": "PBE0/6-311G*"},
            },
        }
    ],
}


def tform(c):
    c.info["per-atom"] = False


name_glob_desc = [
    ("WS22-acrolein", "acrolein", "Configurations of acrolein from WS22. "),
    ("WS22-nitrophenol", "nitrophenol", "Configurations of nitrophenol from WS22. "),
    ("WS22-dmabn", "dmabn", "Configurations of dmabn from WS22. "),
    ("WS22-sma", "sma", "Configurations of sma from WS22. "),
    ("WS22-urea", "urea", "Configurations of urea from WS22. "),
    ("WS22-urocanic", "urocanic", "Configurations of urocanic from WS22. "),
    ("WS22-alanine", "alanine", "Configurations of alanine from WS22. "),
    ("WS22-o-hbdi", "o-hbdi", "Configurations of o-hbdi from WS22. "),
    ("WS22-toluene", "toluene", "Configurations of toluene from WS22. "),
]


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
    ds_id = client.generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    for ds_name, glob, desc in name_glob_desc:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field=None,
            elements=["C", "N", "O", "H"],
            default_name=f"ws22_{glob}",
            reader=reader_ws22,
            glob_string="acrolein",
            verbose=True,
            generator=False,
        )

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            # generator=False,
            transform=tform,
            verbose=False,
        )
    )

    all_cos, all_dos = list(zip(*ids))

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_dos,
        name=ds_name,
        authors=AUTHORS,
        links=LINKS,
        description=desc + DS_DESC,
        resync=True,
        verbose=False,
    )

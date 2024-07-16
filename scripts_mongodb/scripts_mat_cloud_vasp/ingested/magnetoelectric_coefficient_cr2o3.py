"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""

from argparse import ArgumentParser
import functools
import logging
import re
import time
from pathlib import Path
import sys

from ase.atoms import Atoms
from ase.io import read
import pymongo

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/on_the_sign_of_the_linear_magnetoelectric_coefficient_in_cr2o3")
DATASET_NAME = "linear_magnetic_coefficient_in_Cr2O3_JPCM2024"
LICENSE = "CC-BY-4.0"
PUB_YEAR = "2023"

PUBLICATION = "http://doi.org/10.1088/1361-648X/ad1a59"
DATA_LINK = "https://doi.org/10.24435/materialscloud:ek-fp"
# OTHER_LINKS = []

AUTHORS = [
    "Eric Bousquet",
    "Eddy Lelièvre-Berna",
    "Navid Qureshi",
    "Jian-Rui Soh",
    "Nicola Ann Spaldin",
    "Andrea Urru",
    "Xanthe Henderike Verbeek",
    "Sophie Francis Weber",
]
DATASET_DESC = (
    "We establish the sign of the linear magnetoelectric (ME) coefficient, α, "
    "in chromia, Cr₂O₃. Cr₂O₃ is the prototypical linear ME material, in "
    "which an electric (magnetic) field induces a linearly proportional "
    "magnetization (polarization), and a single magnetic domain can be "
    "selected by annealing in combined magnetic (H) and electric (E) fields. "
    "Opposite antiferromagnetic domains have opposite ME responses, and "
    "which antiferromagnetic domain corresponds to which sign of response "
    "has previously been unclear. We use density functional theory (DFT) to calculate "
    "the magnetic response of a single antiferromagnetic domain of Cr₂O₃ to an "
    "applied in-plane electric field at 0 K. We find that the domain with nearest "
    "neighbor magnetic moments oriented away from (towards) each other has a "
    "negative (positive) in-plane ME coefficient, α⊥, at 0 K. We show that this "
    "sign is consistent with all other DFT calculations in the literature that "
    "specified the domain orientation, independent of the choice of DFT code or "
    "functional, the method used to apply the field, and whether the direct "
    "(magnetic field) or inverse (electric field) ME response was calculated. "
    "Next, we reanalyze our previously published spherical neutron polarimetry "
    "data to determine the antiferromagnetic domain produced by annealing in "
    "combined E and H fields oriented along the crystallographic symmetry axis at "
    "room temperature. We find that the antiferromagnetic domain with "
    "nearest-neighbor magnetic moments oriented away from (towards) each other is "
    "produced by annealing in (anti-)parallel E and H fields, corresponding to a "
    "positive (negative) axial ME coefficient, α∥, at room temperature. Since α⊥ at "
    "0 K and α∥ at room temperature are known to be of opposite sign, our "
    "computational and experimental results are consistent. This dataset contains "
    "the input data to reproduce the calculation of the magnetoelectric effect as "
    "plotted in Fig. 3 of the manuscript, for Elk, Vasp, and Quantum Espresso."
)
ELEMENTS = None
GLOB_STR = "OUTCAR"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-LDA"},
    "input": {"field": "input"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "kilobar"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

# CO_METADATA = {
#     "enthalpy": {"field": "h", "units": "Ha"},
#     "zpve": {"field": "zpve", "units": "Ha"},
# }

CSS = [
    [
        f"{DATASET_NAME}_phonon_calculation",
        {"names": {"$regex": "Phonon_calculation"}},
        f"Configurations from {DATASET_NAME} phonon calculations",
    ],
    [
        f"{DATASET_NAME}_structural_relaxation",
        {"names": {"$regex": "Structural_relaxation"}},
        f"Configurations from {DATASET_NAME} structural relaxations",
    ],
    [
        f"{DATASET_NAME}_induced_magnetic_moments",
        {"names": {"$regex": "Induced_magnetic_moments"}},
        f"Configurations from {DATASET_NAME} induced magnetic moment calculations",
    ],
    [
        f"{DATASET_NAME}_born_effective_charges",
        {"names": {"$regex": "Born_effective_charges"}},
        f"Configurations from {DATASET_NAME} born effective charge calculations",
    ],
]


coord_re = re.compile(
    r"^\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+(?P<z>\-?\d+\.\d+)\s+(?P<fx>\-?"
    r"\d+\.\d+)\s+(?P<fy>\-?\d+\.\d+)\s+(?P<fz>\-?\d+\.\d+)"
)
param_re = re.compile(
    r"[\s+]?(?P<param>[A-Z_]+)(\s+)?=(\s+)?(?P<val>-?([\d\w\.\-]+)?\.?)"
    r"[\s;]?(?P<unit>eV)?\:?"
)
IGNORE_PARAMS = [
    "VRHFIN",
    "LEXCH",
    "EATOM",
    "TITEL",
    "LULTRA",
    "IUNSCR",
    "RPACOR",
    "POMASS",
    "RCORE",
    "RWIGS",
    "ENMAX",
    "RCLOC",
    "LCOR",
    "LPAW",
    "EAUG",
    "DEXC",
    "RMAX",
    "RAUG",
    "RDEP",
    "RDEPT",
]


def contcar_parser(fp):
    with open(fp, "r") as f:
        for i in range(5):
            _ = f.readline()
        line = f.readline()
        symbols = line.strip().split()
        counts = [int(x) for x in f.readline().strip().split()]
        symbol_arr = []
        for symbol, count in zip(symbols, counts):
            symbol_arr.extend([symbol] * count)
        return symbol_arr


def namer(fp):
    ds_fp_str = "__".join(DATASET_FP.absolute().parts).replace("/", "")
    name = "__".join(fp.absolute().parts[:-1]).replace("/", "")
    name = name.replace(ds_fp_str + "__", "")
    return name


def convert_stress(keys, stress):
    stresses = {k: s for k, s in zip(keys, stress)}
    return [
        [stresses["xx"], stresses["xy"], stresses["zx"]],
        [stresses["xy"], stresses["yy"], stresses["yz"]],
        [stresses["zx"], stresses["yz"], stresses["zz"]],
    ]


def outcar_reader(symbols, fp):
    with open(fp, "r") as f:
        configs = []
        incar = dict()
        cinput = dict()
        in_latt = False
        in_coords = False
        lattice = []
        pos = []
        forces = []
        stress = None
        potcars = set()
        energy = None
        for line in f:
            # Prelim handling
            if line.strip() == "":
                pass

            # handle lattice
            elif "direct lattice vectors" in line:
                in_latt = True
                lattice = []
                pass
            elif in_latt is True:
                latt = line.strip().replace("-", " -").split()
                lattice.append([float(x) for x in [latt[0], latt[1], latt[2]]])
                if len(lattice) == 3:
                    in_latt = False
            elif "POTCAR:" in line:
                potcars.add(" ".join(line.strip().split()[1:]))
            elif "FREE ENERGIE OF THE ION-ELECTRON SYSTEM" in line:
                _ = f.readline()
                _, _, _, _, energy, units = f.readline().strip().split()
                if len(pos) > 0:
                    cinput["incar"] = incar
                    config = Atoms(positions=pos, symbols=symbols, cell=lattice)
                    config.info["input"] = cinput
                    config.info["input"]["potcars"] = list(potcars)
                    # config.info["outcar"] = outcar
                    if stress is not None:
                        config.info["stress"] = stress
                    config.info["forces"] = forces
                    config.info["energy"] = float(energy)
                    configs.append(config)
                    forces = []
                    stress = None
                    pos = []
                    energy = None
            elif "POSITION" in line:
                in_coords = True
                pass
            elif in_coords is True:
                if "--------" in line:
                    pass
                elif "total drift" in line:
                    in_coords = False
                    if energy is not None:
                        cinput["incar"] = incar
                        config = Atoms(positions=pos, symbols=symbols, cell=lattice)
                        config.info["input"] = cinput
                        config.info["input"]["potcars"] = list(potcars)
                        # config.info["outcar"] = outcar
                        config.info["forces"] = forces
                        if stress is not None:
                            config.info["stress"] = stress
                        config.info["energy"] = float(energy)
                        configs.append(config)
                        forces = []
                        stress = None
                        pos = []
                        energy = None
                    else:
                        pass
                else:
                    cmatch = coord_re.search(line)
                    pos.append(
                        [float(p) for p in [cmatch["x"], cmatch["y"], cmatch["z"]]]
                    )
                    forces.append(
                        [float(p) for p in [cmatch["fx"], cmatch["fy"], cmatch["fz"]]]
                    )
            elif "Direction" in line and "XX" in line:
                stress_keys = [x.lower() for x in line.strip().split()[1:]]
            elif "in kB" in line:
                stress = [float(x) for x in line.strip().split()[2:]]
                stress = convert_stress(stress_keys, stress)

            else:
                pass
                # print("something went wrong")

    return configs


def get_kpoints(fp):
    with open(fp, "r") as f:
        # f.readline() # if skipping first line
        kpoints = "".join(f.readlines())
    return kpoints


def parse_incar(fp):
    with open(fp, "r") as f:
        return f.read()


def file_finder(fp, file_glob, count=0):
    if count > 5:
        return None
    elif next(fp.glob(file_glob), None) is not None:
        # file_glob in [f.name for f in fp.glob("*")]:
        return next(fp.glob(file_glob))
    else:
        count += 1
        return file_finder(fp.parent, file_glob, count)


def xml_reader(filepath: Path):
    # name = "_".join(filepath.parts[-4:-1])
    name = namer(filepath)
    data = read(filepath, index=":", format="vasp-xml")
    for config in data:
        config.info["forces"] = config.get_forces()
        config.info["energy"] = config.get_total_energy()

        config.info["stress"] = config.get_stress(voigt=False)
        config.info["name"] = name
        yield config


def outcar_wrapper(filepath: Path):
    name = namer(filepath)
    poscar = next(filepath.parent.glob(filepath.name.replace("OUTCAR", "POSCAR")))
    symbols = contcar_parser(poscar)
    kpoints_file = file_finder(filepath.parent, "KPOINTS")
    kpoints = get_kpoints(kpoints_file)
    incar_file = file_finder(filepath.parent, "INCAR")
    incar = parse_incar(incar_file)

    configs = outcar_reader(fp=filepath, symbols=symbols)
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
        config.info["input"]["kpoints"] = kpoints
        config.info["input"]["incar"] = incar

    return configs


MAX_AUTO_RECONNECT_ATTEMPTS = 100


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                if wait_t > 1800:
                    wait_t = 1800  # cap at 1/2 hour
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


@auto_reconnect
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()
    print("DS-ID: ", ds_id, "\nDS-NAME: ", DATASET_NAME)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=xml_reader,
        glob_string="*.xml",
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            # co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=outcar_wrapper,
        glob_string="OUTCAR*",
        generator=False,
    )
    ids.extend(
        list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                # co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=True,
            )
        )
    )
    all_co_ids, all_do_ids = list(zip(*ids))

    cs_ids = []
    for i, (name, query, desc) in enumerate(CSS):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=query,
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        other_links=None,
        description=DATASET_DESC,
        publication_year=PUB_YEAR,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )
    client.close()


if __name__ == "__main__":
    main(sys.argv[1:])

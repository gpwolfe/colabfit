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
import pymongo

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path(
    "data/enlisting_potential_cathode_materials_for_rechargeable_ca_batteries"
)
DATASET_NAME = "cathode_materials_for_rechargeable_Ca_batteries_CM2021"
LICENSE = "CC-BY-4.0"
PUB_YEAR = "2021"

PUBLICATION = "http://doi.org/10.1038/s41598-019-46002-4"
DATA_LINK = "https://doi.org/10.24435/materialscloud:3n-e8"
# OTHER_LINKS = []

AUTHORS = ["M. Elena Arroyo-de Dompablo", "Jose Luis Casals"]
DATASET_DESC = (
    "Data from the publication "
    '"Enlisting Potential Cathode Materials for Rechargeable Ca Batteries". '
    "The development of rechargeable batteries based on a Ca metal anode demands "
    "the identification of suitable cathode materials. This work investigates the "
    "potential application of a variety of compounds, which are selected from the "
    "In-organic Crystal Structural Database (ICSD) considering 3d-transition metal "
    "oxysulphides, pyrophosphates, silicates, nitrides, and phosphates with a maximum "
    "of four different chemical elements in their composition. Cathode perfor-mance "
    "of CaFeSO, CaCoSO, CaNiN, Ca3MnN3, Ca2Fe(Si2O7), CaM(P2O7) (M = V, Cr, Mn, Fe, "
    "Co), CaV2(P2O7)2, Ca(VO)2(PO4)2 and α-VOPO4 is evaluated throughout the "
    "calculation of operation voltages, volume changes associated to the redox "
    "reaction and mobility of Ca2+ ions. Some materials exhibit attractive specific "
    "capacities and intercalation voltages combined with energy barriers for Ca "
    "migration around 1 eV (CaFeSO, Ca2FeSi2O7 and CaV2(P2O7)2). Based on the DFT "
    "results, αI-VOPO4 is identified as a potential Ca-cathode with a maximum "
    "theoretical specific capacity of 312 mAh/g, an average intercalation voltage "
    "of 2.8 V and calculated energy barriers for Ca migration below 0.65 eV "
    "(GGA functional)."
)
ELEMENTS = None
GLOB_STR = "OUTCAR"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
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
symbol_map = {
    "": ["Ca", "V", "P", "O"],
    "Ca48Mn16O48": ["Ca", "Mn", "O"],
    "ca3mnn3": ["Ca", "Mn", "N"],
    "Ca36Ni36N36": ["Ca", "Ni", "N"],
    "CaNiN": ["Ca", "N", "Ni"],
    "icsd72886": ["Ca", "V", "P", "O"],
    "sc222desdecif": ["Ca", "Mn", "P", "O"],
    "sc311": ["Ca", "Fe", "S", "O"],
    "desdeelprimdenavpo4": ["Ca", "V", "P", "O"],
    "sc222x05": ["Ca", "V", "P", "O"],
    "LiVOPO499618": ["V", "P", "O"],
    "duplicado_contcar_opt0": ["Ca", "V", "P", "O"],
    "sc221ca2cosi2o7": ["Ca", "Co", "Si", "O"],
    "scdesdeicsdco": ["Ca", "Fe", "Si", "O"],
    "melilite_imple-icsd31236": ["Ca", "Fe", "Si", "O"],
    "sc311de74554": ["Ca", "V", "P", "O"],
    "desdex1ggacontcar": ["Ca", "V", "P", "O"],
    "icsd74554": ["Ca", "V", "P", "O"],
    "cafeso_251820": ["Ca", "Fe", "S", "O"],
}

CSS = [
    [
        f"{DATASET_NAME}_ICSD_cloud_CaFeSO",
        {"names": {"$regex": "^ICSD_cloud__cafeso"}},
        f"Configurations from CaFeSO calculations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_ICSD_cloud_CaV2P4O14",
        {"names": {"$regex": "^ICSD_cloud__CaV2P4O14"}},
        f"Configurations from CaV2P4O14 calculations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_ICSD_cloud_melilite",
        {"names": {"$regex": "^ICSD_cloud__melilite"}},
        f"Configurations from melilite calculations from {DATASET_NAME} dataset,()",
    ],
    [
        f"{DATASET_NAME}_ICSD_cloud_VPO",
        {"names": {"$regex": "^ICSD_cloud__VPO"}},
        f"Configurations from VPO calculations from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_ICSD_SI_cloud_Ca3MnN3",
        {"names": {"$regex": "SI_ICSD_cloud__Ca3MnN3"}},
        f"Configurations from Ca3MnN3 calculations from {DATASET_NAME} "
        "dataset, SI data",
    ],
    [
        f"{DATASET_NAME}_ICSD_SI_cloud_CaCoSO",
        {"names": {"$regex": "SI_ICSD_cloud__cacoso"}},
        f"Configurations from CaCoSO calculations from {DATASET_NAME} "
        "dataset, SI data",
    ],
    [
        f"{DATASET_NAME}_ICSD_SI_cloud_CaMnP2O7",
        {"names": {"$regex": "SI_ICSD_cloud__CaMP2O7"}},
        f"Configurations from CaMnP2O7 calculations from {DATASET_NAME} "
        "dataset, SI data",
    ],
    [
        f"{DATASET_NAME}_ICSD_SI_cloud_CaNiN",
        {"names": {"$regex": "SI_ICSD_cloud__CaNiN"}},
        f"Configurations from CaNiN calculations from {DATASET_NAME} "
        "dataset, SI data",
    ],
    [
        f"{DATASET_NAME}_ICSD_SI_cloud_CaV2P2O10",
        {"names": {"$regex": "SI_ICSD_cloud__CaV2P2O10"}},
        f"Configurations from CaV2P2O10 calculations from {DATASET_NAME} "
        "dataset, SI data",
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
        header = f.readline()
        for key, val in symbol_map.items():
            if key in header:
                symbols = val

        for i in range(4):
            _ = f.readline()
        counts = [x for x in f.readline().strip().split()]
        if not counts[0].isdigit():
            counts = [x for x in f.readline().strip().split()]
        counts = [int(x) for x in counts]
        if len(counts) < len(symbols):
            symbols = [s for s in symbols if s != "Ca"]

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
            elif "Direction" in line and " X " in line:
                stress_keys = [x.lower() for x in line.strip().split()[1:]]
                for i, key in enumerate(stress_keys):
                    if len(key) == 1:
                        stress_keys[i] = key + key
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
        #     lines = f.readlines()
        # incar = dict()
        # for line in lines:
        #     if "=" in line:
        #         keyvals = line.split("=")
        #         key = keyvals[0].strip()
        #         value = "".join(keyvals[1:]).strip().split("#")[0].strip()
        #         incar[key] = value
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


def reader(filepath: Path):
    print(filepath)
    name = namer(filepath)
    poscar = next(filepath.parent.glob(filepath.name.replace("OUTCAR", "CONTCAR")))
    symbols = contcar_parser(poscar)
    if "cacoso" in name:
        symbols = [s if s != "Fe" else "Co" for s in symbols]
    kpoints_file = file_finder(filepath.parent, "KPOINTS")
    if kpoints_file is not None:
        kpoints = get_kpoints(kpoints_file)
    else:
        kpoints = "no kpoints file found"
    incar_file = file_finder(filepath.parent, "INCAR")
    if incar_file is not None:
        incar = parse_incar(incar_file)
    else:
        incar = "no incar file found"

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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
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


if __name__ == "__main__":
    main(sys.argv[1:])

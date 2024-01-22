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
from pathlib import Path
import re
import sys

# from ase.io import read
from ase.atoms import Atoms

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/co2_hydrogenation_mgo2")
DATASET_NAME = (
    "water_and_Cu+_synergy_in_selective_CO2_hydrogenation_"
    "to_methanol_over_Cu/MgO_catalysts"
)
LICENSE = "https://creativecommons.org/licenses/by/4.0"

PUBLICATION = "https://doi.org/10.1021/jacs.3c10685"
DATA_LINK = "https://doi.org/10.24435/materialscloud:tz-pn"
# OTHER_LINKS = []

AUTHORS = [
    "Estefanía Fernández Villanueva",
    "Pablo Germán Lustemberg",
    "Minjie Zhao",
    "Jose Soriano",
    "Patricia Concepción",
    "María Verónica Ganduglia Pirovano",
]
DATASET_DESC = (
    "This dataset was created to investigate the role of surface water and "
    "hydroxyl groups in facilitating spontaneous CO₂ activation at Cu⁺ sites and "
    "the formation of monodentate formate species in the context of using CO2 "
    "hydrogenation to produce methanol."
)
ELEMENTS = None
GLOB_STR = "OUTCAR*"

PI_METADATA = {
    "software": {"value": "VASP 6.3.0"},
    "method": {"value": "DFT-PBE-D3"},
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
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "eV/angstrom^3"},
    #         "volume-normalized": {"value": False, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}

CO_METADATA = {
    "outcar": {"field": "outcar"},
}

# CSS = [
#     [
#         f"{DATASET_NAME}_aluminum",
#         {"names": {"$regex": "aluminum"}},
#         f"Configurations of aluminum from {DATASET_NAME} dataset",
#     ]
# ]
latt_re = re.compile(
    r"A\d = \(\s+(?P<a>\-?\d+\.\d+),\s+(?P<b>\-?\d+\.\d+),\s+(?P<c>\-?\d+\.\d+)\)"
)
latt_typ_re = re.compile(r"\s+LATTYP: Found a (?P<latt_type>[\S\s]+) cell.\n")
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

latt_re = re.compile(
    r"A\d = \(\s+(?P<a>\-?\d+\.\d+),\s+(?P<b>\-?\d+\.\d+),\s+(?P<c>\-?\d+\.\d+)\)"
)
latt_typ_re = re.compile(r"\s+LATTYP: Found a (?P<latt_type>[\S\s]+) cell.\n")
coord_re = re.compile(
    r"^\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+(?P<z>\-?\d+\.\d+)\s+(?P<fx>\-?"
    r"\d+\.\d+)\s+(?P<fy>\-?\d+\.\d+)\s+(?P<fz>\-?\d+\.\d+)"
)
param_re = re.compile(
    r"[\s+]?(?P<param>[A-Z_]+)(\s+)?=(\s+)?(?P<val>-?([\d\w\.\-]+)?\.?)"
    r"[\s;]?(?P<unit>eV)?\:?"
)
lattice_re = re.compile(r"(?P<a>\d+\.\d+)\s+(?P<b>\d+\.\d+)\s+(?P<c>\d+\.\d+)")


def contcar_parser(fp):
    lattice = []
    symbol_counts = dict()
    with open(fp, "r") as f:
        for i in range(5):
            _ = f.readline()
        line = f.readline()
        symbols = line.strip().split()
        counts = [int(x) for x in f.readline().strip().split()]
        symbol_counts = dict(zip(symbols, counts))
        symbols = []
        for symbol in symbol_counts:
            symbols.extend([symbol] * symbol_counts[symbol])
        return lattice, symbols


def outcar_reader(symbols, fp):
    with open(fp, "r") as f:
        configs = []
        incar = dict()
        cinput = dict()
        outcar = dict()
        in_incar = False
        in_latt = False
        in_coords = False
        lattice = []
        pos = []
        forces = []
        settings = True
        for line in f:
            if "conjugate gradient relaxation of ions" in line:
                settings = False
                pass
            # Prelim handling
            elif line.strip() == "":
                pass

            # handle lattice
            elif "Lattice vectors" in line:
                in_latt = True
                pass
            elif in_latt is True:
                if len(lattice) == 3:
                    in_latt = False
                    pass
                else:
                    latt_match = latt_re.search(line)
                    lattice.append(
                        [
                            float(x)
                            for x in [latt_match["a"], latt_match["b"], latt_match["c"]]
                        ]
                    )

            # handle incar

            elif "INCAR" in line:
                in_incar = True
                continue

            elif in_incar is True:
                if "direct lattice vectors" in line:
                    in_incar = False
                    pass
                elif "POTCAR" in line:
                    incar["POTCAR"] = " ".join(line.strip().split()[1:])
                else:
                    for pmatch in param_re.finditer(
                        line
                    ):  # sometimes more than one param/line
                        # param, val, unit
                        if pmatch["unit"] is not None:
                            incar[pmatch["param"]] = {
                                "value": float(pmatch["val"]),
                                "units": pmatch["unit"],
                            }
                        else:
                            incar[pmatch["param"]] = pmatch["val"]
            # handle coords/nums
            elif "POSITION" in line:
                in_coords = True
                pass
            elif in_coords is True:
                if "--------" in line:
                    pass
                elif "total drift" in line:
                    in_coords = False
                    pass
                else:
                    cmatch = coord_re.search(line)
                    pos.append(
                        [float(p) for p in [cmatch["x"], cmatch["y"], cmatch["z"]]]
                    )
                    forces.append(
                        [float(p) for p in [cmatch["fx"], cmatch["fy"], cmatch["fz"]]]
                    )
            elif "FREE ENERGIE OF THE ION-ELECTRON SYSTEM" in line:
                _ = f.readline()
                _, _, _, _, energy, units = f.readline().strip().split()
                cinput["incar"] = incar
                config = Atoms(positions=pos, symbols=symbols, cell=lattice)
                config.info["input"] = cinput
                config.info["outcar"] = outcar
                config.info["forces"] = forces
                config.info["energy"] = float(energy)
                config.info["name"] = f"{'__'.join(fp.parts[-4:-1])}"
                configs.append(config)
                forces = []
                pos = []
                energy = None
            # Check other lines for params, send to outcar or cinput
            elif settings is True:
                for pmatch in param_re.finditer(line):
                    if pmatch["param"] in IGNORE_PARAMS:
                        pass
                    elif pmatch["unit"] is not None:
                        cinput[pmatch["param"]] = {
                            "value": float(pmatch["val"]),
                            "units": pmatch["unit"],
                        }
                    elif pmatch["unit"] is None:
                        cinput[pmatch["param"]] = pmatch["val"]
            elif settings is False:
                for pmatch in param_re.finditer(line):
                    if pmatch["unit"] is not None:
                        outcar[pmatch["param"]] = {
                            "value": float(pmatch["val"]),
                            "units": pmatch["unit"],
                        }
                    elif pmatch["unit"] is None:
                        outcar[pmatch["param"]] = pmatch["val"]
            else:
                print("something went wrong")

        return configs


def reader(filepath: Path):
    contcar = next(filepath.parent.glob("CONTCAR*"))

    lattice, symbols = contcar_parser(contcar)
    configs = outcar_reader(symbols, filepath)
    return configs


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
    # client.insert_property_definition(cauchy_stress_pd)

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
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=False,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # cs_ids = []
    # for i, (name, query, desc) in enumerate(CSS):
    #     cs_id = client.query_and_insert_configuration_set(
    #         co_hashes=all_co_ids,
    #         ds_id=ds_id,
    #         name=name,
    #         description=desc,
    #         query=query,
    #     )

    #     cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=False,
        # cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

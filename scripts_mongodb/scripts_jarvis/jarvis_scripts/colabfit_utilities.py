from ase import Atoms
from argparse import ArgumentParser
from colabfit.tools.converters import AtomicConfiguration
from colabfit.tools.database import MongoDatabase
from collections import defaultdict
import numpy as np
from pathlib import Path
import re


def get_client_notebook(ip, port, db_name, nprocs):
    client = MongoDatabase(db_name, nprocs=nprocs, uri=f"mongodb://{ip}:{port}")
    return client


def get_client(argv):
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
    return client


####################################################################################
"""
MLIP
Reader function and functions used within reader function for MLIP-formatted .cfg files
The manual for MLIP can be viewed online here:
https://gitlab.com/ashapeev/mlip-2-paper-supp-info/-/blob/master/manual.pdf

Note that stresses are virials multiplied by cell volume. Units are eV
Energy: eV
Forces: eV/A
Coordinates may be cartesian or 'direct'. This is handled in the reader function

Ex. file:
BEGIN_CFG
 Size
    4
 Supercell
    4.3499999999999996e+00 0.0000000000000000e+00 0.0000000000000000e+00
    0.0000000000000000e+00 4.3499999999999996e+00 0.0000000000000000e+00
    0.0000000000000000e+00 0.0000000000000000e+00 4.3499999999999996e+00
 AtomData:  id type       cartes_x      cartes_y    cartes_z  fx   fy   fz
    1 0  [corresponding float values...]
    2 0  [corresponding float values...]
    3 0  [corresponding float values...]
    4 0  [corresponding float values...]
 Energy
    -2.4831664090000000e+01
 PlusStress:  xx          yy          zz          yz          xz          xy
     [6 float values...]
 Feature   EFS_by	VASP
 Feature   mindist	3.075914
END_CFG
"""
SYMBOL_DICT = {"0": "Zr", "1": "Sn"}


def convert_stress(keys, stress):
    stresses = {k: s for k, s in zip(keys, stress)}
    return [
        [stresses["xx"], stresses["xy"], stresses["xz"]],
        [stresses["xy"], stresses["yy"], stresses["yz"]],
        [stresses["xz"], stresses["yz"], stresses["zz"]],
    ]


def reader(filepath):
    with open(filepath, "rt") as f:
        energy = None
        forces = None
        coords = []
        cell = []
        symbols = []
        config_count = 0
        for line in f:
            if line.strip().startswith("Size"):
                size = int(f.readline().strip())
            elif line.strip().lower().startswith("supercell"):
                cell.append([float(x) for x in f.readline().strip().split()])
                cell.append([float(x) for x in f.readline().strip().split()])
                cell.append([float(x) for x in f.readline().strip().split()])
            elif line.strip().startswith("Energy"):
                energy = float(f.readline().strip())
            elif line.strip().startswith("PlusStress"):
                stress_keys = line.strip().split()[-6:]
                stress = [float(x) for x in f.readline().strip().split()]
                stress = convert_stress(stress_keys, stress)
            elif line.strip().startswith("AtomData:"):
                keys = line.strip().split()[1:]
                if "fx" in keys:
                    forces = []
                for i in range(size):
                    li = {
                        key: val for key, val in zip(keys, f.readline().strip().split())
                    }
                    symbols.append(SYMBOL_DICT[li["type"]])
                    if "cartes_x" in keys:
                        coords.append(
                            [
                                float(c)
                                for c in [
                                    li["cartes_x"],
                                    li["cartes_y"],
                                    li["cartes_z"],
                                ]
                            ]
                        )
                    elif "direct_x" in keys:
                        coords.append(
                            [
                                float(c)
                                for c in [
                                    li["direct_x"],
                                    li["direct_y"],
                                    li["direct_z"],
                                ]
                            ]
                        )

                    if forces:
                        forces.append(
                            [float(f) for f in [li["fx"], li["fy"], li["fz"]]]
                        )

            elif line.startswith("END_CFG"):
                if "cartes_x" in keys:
                    config = AtomicConfiguration(
                        positions=coords, symbols=symbols, cell=cell
                    )
                elif "direct_x" in keys:
                    config = AtomicConfiguration(
                        scaled_positions=coords, symbols=symbols, cell=cell
                    )
                config.info["energy"] = energy
                if forces:
                    config.info["forces"] = forces
                config.info["stress"] = stress
                config.info["name"] = f"{filepath.stem}_{config_count}"
                config_count += 1
                yield config
                forces = None
                stress = []
                coords = []
                cell = []
                symbols = []
                energy = None


####################################################################################
"""
N2P2
Reader function and regexes used in parsing n2p2 formatted files
"""

ATOM_RE = re.compile(
    r"atom\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+"
    r"(?P<z>\-?\d+\.\d+)\s+(?P<element>\w{1,2})\s+0.0+\s+0.0+\s+(?P<f1>\-?\d+\.\d+)"
    r"\s+(?P<f2>\-?\d+\.\d+)\s+(?P<f3>\-?\d+\.\d+)"
)
LATT_RE = re.compile(
    r"lattice\s+(?P<lat1>\-?\d+\.\d+)\s+(?P<lat2>\-?\d+\.\d+)\s+(?P<lat3>\-?\d+\.\d+)"
)
EN_RE = re.compile(
    r"comment\s+i\s=\s+\d+,\s+time\s=\s+\-?\d+\.\d+,\s+E\s=\s+(?P<energy>\-?\d+\.\d+)"
)


def n2p2_reader(filepath):
    with open(filepath) as f:
        configurations = []
        lattice = []
        coords = []
        forces = []
        elements = []
        counter = 0
        for line in f:
            if (
                line.startswith("begin")
                # or line.startswith("end")
                or line.startswith("charge")
                or line.startswith("energy")
            ):
                pass
            elif line.startswith("lattice"):
                lattice.append([float(x) for x in LATT_RE.match(line).groups()])
            elif line.startswith("atom"):
                ln_match = ATOM_RE.match(line)
                coords.append([float(x) for x in ln_match.groups()[:3]])
                forces.append([float(x) for x in ln_match.groups()[-3:]])
                elements.append(ln_match.groups()[3])
            elif line.startswith("comment"):
                energy = float(EN_RE.match(line).group("energy"))
            elif line.startswith("end"):
                config = AtomicConfiguration(
                    positions=coords, symbols=elements, cell=lattice
                )
                config.info["forces"] = forces
                config.info["energy"] = energy
                config.info["name"] = f"{filepath.parts[-2]}_{counter}"
                configurations.append(config)
                # if counter == 100:  # comment after testing
                #     return configurations  # comment after testing
                counter += 1
                lattice = []
                coords = []
                forces = []
                elements = []
    return configurations


###########################################################################
# A cheap version of a property instance filter as a generator
# The old filter_on_property function from colabfit.tools.database.MongoDatabase
# no longer works, since it relied on bidirectional pointers from dataset to property
# instances, and so on.
# This would replace the function in the forementioned class, or can be called with
# client = MongoDatabase(...)
# filter_on_properties(self=client, ...)


def filter_on_properties(self, ds_id, query=None):
    """
    Returns a generator of property instances from given dataset that match query

    Aggregator function performs, in order:
    $match on data objects that point to given dataset id (colabfit-id)
    $lookup of property instances that point to those data object ids
    $match based on property query: such as {"type": "free-energy"}

    """
    agg_pipe = self.data_objects.aggregate(
        [
            {"$match": {"relationships.datasets": ds_id}},
            {
                "$lookup": {
                    "from": "property_instances",
                    "foreignField": "relationships.data_objects",
                    "localField": "colabfit-id",
                    "as": "pi_data",
                }
            },
            {"$unwind": "$pi_data"},
            {"$match": {f"pi_data.{field}": val for field, val in query.items()}},
            {"$project": {"_id": 0, "pi_data": "$pi_data"}},
        ]
    )
    for datapoint in agg_pipe:
        yield datapoint["pi_data"]


###########################################################################
# Function for reading numpy npz files


def read_npz(filepath):
    data = defaultdict(list)
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
    return data


###########################################################################
# When adapting the scripts to edit from Eric/Alexander, generally can use this block
# instead of the old configuration set loading block


def just_for_the_linter(regex, client, all_co_ids, cs_names, cs_regexes):
    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=cs_names[i],
            description=desc,
            query={"names": {"$regex": regex}},
        )
        cs_ids.append(cs_id)


###########################################################################
# A workaround for datasets that have too many configurations for computer memory
# to hold all at once, as long as there are multiple files scattered through
# sufficient directories to divide the task

# globs = list(set([db.parent for db in DATASET_FP.rglob(GLOB_STR)]))
# configurations = load_data(
#     file_path=globs[0],
#     file_format="folder",
#     name_field="name",
#     elements=ELEMENTS,
#     reader=reader,
#     glob_string=GLOB_STR,
#     generator=False,
# )
# ids = list(
#     client.insert_data(
#         configurations,
#         property_map=property_map,
#         generator=False,
#         verbose=True,
#     )
# )
# for gl in globs:
#     configurations = load_data(
#         file_path=gl,
#         file_format="folder",
#         name_field="name",
#         elements=ELEMENTS,
#         reader=reader,
#         glob_string=GLOB_STR,
#         generator=False,
#     )

#     ids.extend(
#         client.insert_data(
#             configurations,
#             property_map=property_map,
#             generator=False,
#             verbose=True,
#         )
#     )

###########################################################################

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

    # if type_map.raw file is present, use below to create on-the-fly atom type map
    # elem_map = dict()
    # with open(filepath.parents[1] / "type_map.raw", "r") as f:
    #     types = [x.strip() for x in f.readlines()]
    #     for i, t in enumerate(types):
    #         elem_map[i] = t

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


def npy_reader(filepath):
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


###########################################################################


def assemble_npy_properties(filepath: Path):
    prop_path = filepath.parent.glob("*.npy")
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


###########################################################################


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


###########################################################################


# def assemble_np(fp_dict, props: dict):
#     """
#     fp_dict: dictionary with filepaths as keys and the target property
#         (as defined by the keys in props below) as value
#     props: dictionary containing keys equal to the keys outlined below and
#         values equal to the equivalent numpy keys given by <filename>.files
#     props = {
#         'name': <name>,
#         'coords': <key of coordinates>,
#         'energy': <key of potential energy>,
#         'forces': <key of forces>,
#         'cell': <key of cell/lattice>,
#         'pbc': <PBC True or False (set by user)>,
#         'numbers': <key for atomic numbers>,
#         'elements': <key for atomic elements
#     }
#     """
#     file_props = {}

#     for fp, val in fp_dict.items():
#         data = np.load(fp)
#         # in case the file only contains, for instance, a single float value
#         if not props.get(val):
#             file_props[val] = data
#         else:
#             file_props[val] = data[props[val]]
#     return file_props


# cs_regexes = [
#     [
#         "H2_H2/Pt(III)",
#         "H2*",
#         "H2 configurations from H/Pt(III)",
#     ],
# ]

# cs_ids = []

# for i, (name, regex, desc) in enumerate(cs_regexes):
#     try:
#         co_ids = client.get_data(
#             "configurations",
#             fields="hash",
#             query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
#             ravel=True,
#         ).tolist()
#     except OperationFailure:
#         print(f"No match for regex: {regex}")
#         continue

#     print(
#         f"Configuration set {i}",
#         f"({name}):".rjust(25),
#         f"{len(co_ids)}".rjust(7),
#     )

#     if len(co_ids) == 0:
#         pass
#     else:
#         cs_id = client.insert_configuration_set(co_ids, description=desc, name=name)

#         cs_ids.append(cs_id)

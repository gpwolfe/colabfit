"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.5281/zenodo.7196578

File address:
https://zenodo.org/record/7196767/files/mdsim_data.tar.gz?download=1


Extract/move to project folder
tar -zxf mdsim_data.tar.gz -C $project_dir/scripts/forces_are_not_enough/
mv water.npy alanine_dipeptide.npy $project_dir/scripts/forces_are_not_enough/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:


Other properties added to metadata:


File notes
----------
alanine 'angles' are a single 3-vector [90.0, 90.0, 90.0]
alanine data in 'mdsim_data/ala' directory contains no energy data

Not included:
the "source data": flexwater.npy and ala.npy
The authors describe the rest of the files (which we ARE importing)
as their 'preprocessed' data
There are, however, two functions to read these files, commented out at
the bottom of this script. Again, ala.npy contains no energy data

Not downloaded:
water.npy and alanine_dipeptide.npy are exact copies of flexwater.npy and
ala.npy (contained in mdsim_data.tar.gz), and are therefore not downloaded




"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import re
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/forces_are_not_enough"
)
DATASET_FP = Path().cwd().parent / "data/forces_are_not_enough"
ALA_FP = DATASET_FP / "mdsim_data/ala/40k/DP/test/set.000/"
WATER_FP = DATASET_FP / "mdsim_data/water"
LIPS_FP = DATASET_FP / "mdsim_data/lips"
MD17_FP = DATASET_FP / "mdsim_data/md17"

DATASET = "Forces_are_not_enough"
PUBLICATION = "https://doi.org/10.48550/arXiv.2210.07237"
DATA_LINK = "https://doi.org/10.5281/zenodo.7196767"
OTHER_LINKS = ["https://github.com/kyonofx/MDsim/"]

LINKS = [
    "https://doi.org/10.5281/zenodo.7196767",
    "https://doi.org/10.48550/arXiv.2210.07237",
    "https://github.com/kyonofx/MDsim/",
]
AUTHORS = [
    "Xiang Fu",
    "Zhenghao Wu",
    "Wujie Wang",
    "Tian Xie",
    "Sinan Keten",
    "Rafael Gomez-Bombarelli",
    "Tommi Jaakkola",
]
DS_DESC = (
    "Approximately 300,000 benchmarking configurations "
    "derived partly from the MD-17 and LiPS datasets, partly from "
    "original simulated water and alanine dipeptide configurations."
)

RE = re.compile(r"")

ELEM_KEY = {
    "ala": (("H", "C", "N", "O"), "AMBER-03", "GROMACS"),
    "lips": (("Li", "P", "S"), "DFT-PBE", "VASP"),
    "aspirin": (("H", "C", "O"), "DFT-PBE-vdW-TS", "i-PI"),
    "benzene": (("H", "C"), "DFT-PBE-vdW-TS", "i-PI"),
    "ethanol": (("H", "C", "O"), "DFT-PBE-vdW-TS", "i-PI"),
    "malonaldehyde": (("H", "C", "O"), "DFT-PBE-vdW-TS", "i-PI"),
    "naphthalene": (("H", "C"), "DFT-PBE-vdW-TS", "i-PI"),
    "salicylic_acid": (("H", "C", "O"), "DFT-PBE-vdW-TS", "i-PI"),
    "toluene": (("H", "C"), "DFT-PBE-vdW-TS", "i-PI"),
    "uracil": (("H", "C", "N", "O"), "DFT-PBE-vdW-TS", "i-PI"),
    "water": (("H", "O"), "NPT-PME-SHAKE", "DLPOLY"),
}


def assemble_props(filepath: Path):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    type_path = list(filepath.parents[1].glob("type.raw"))[0]
    for key in ELEM_KEY:
        if key in type_path.parts[-7:]:
            elem_key = ELEM_KEY[key][0]
            methods = ELEM_KEY[key][1]
            software = ELEM_KEY[key][2]
    with open(type_path, "r") as f:
        nums = f.read().split(" ")
        props["symbols"] = [elem_key[int(num)] for num in nums]

    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p)
    num_configs = props["force"].shape[0]
    num_atoms = len(props["symbols"])
    props["force"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    props["methods"] = methods
    props["software"] = software
    return props


def reader(filepath):
    props = assemble_props(filepath)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["force"][i]
        c.info["software"] = props["software"]
        c.info["methods"] = props["methods"]
        # alanine has no energy data
        if energy is not None:
            c.info["energy"] = float(energy[i])
        c.info[
            "name"
        ] = f"{filepath.parts[-6]}_{filepath.parts[-5]}_{filepath.parts[-3]}_{i}"
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

    ala_configs = load_data(
        file_path=ALA_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="box.npy",
        generator=False,
    )
    configurations = load_data(
        file_path=WATER_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        reader=reader,
        glob_string="box.npy",
        generator=False,
    )
    configurations.extend(
        load_data(
            file_path=LIPS_FP,
            file_format="folder",
            name_field="name",
            elements=["Li", "S", "P"],
            reader=reader,
            glob_string="box.npy",
            generator=False,
        )
    )
    configurations.extend(
        load_data(
            file_path=MD17_FP,
            file_format="folder",
            name_field="name",
            elements=["C", "H", "O", "N"],
            reader=reader,
            glob_string="box.npy",
            generator=False,
        )
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"field": "software"},
        "method": {"field": "methods"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/angstrom"},
                "_metadata": metadata,
            }
        ],
    }
    ala_property_map = {
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/angstrom"},
                "_metadata": metadata,
            }
        ]
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=False,
        )
    )
    ala_ids = list(
        client.insert_data(
            ala_configs,
            ds_id=ds_id,
            property_map=ala_property_map,
            generator=False,
            verbose=False,
        )
    )
    ids.extend(ala_ids)
    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [key, f"{key}*", f"{key} configurations from {DATASET} dataset"]
        for key in ELEM_KEY
        if key != "ala"
    ]
    cs_regexes.append(
        [
            "alanine-dipeptide",
            "ala*",
            f"alanine dipeptide configurations from {DATASET} dataset",
        ]
    )

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={
                "hash": {"$in": all_co_ids},
                "names": {"$regex": regex},
            },
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )
        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, ds_id=ds_id, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

# def source_water_reader(file_path):
#     file_path = Path(file_path)
#     data = np.load(file_path, allow_pickle=True)
#     data = data.tolist()
#     # keys: 'wrapped_coords', 'unwrapped_coords', 'forces', 'velocities'
#     # 'lengths', 'angles', 'raw_types', 'atom_types', 'bond_index',
#     # 'bond_types', 'e_steps', 'energy'
#     atoms = []
#     atoms = [AtomicConfiguration(numbers=data['atom_types'],
#              positions=pos) for i, pos in enumerate(data['wrapped_coords'])]
#     for i, atom in enumerate(atoms):
#         atom.info['energy'] = data['energy'][i]
#         atom.info['forces'] = data['forces'][i]
#         atom.info['unwrapped_coords'] = data['unwrapped_coords'][i]
#         atom.info['velocities'] = data['velocities'][i]
#         atom.info['lengths'] = data['lengths'][i]
#         atom.info['angles'] = data['angles'][i]
#         atom.info['e_steps'] = data['e_steps'][i]
#         atom.info['name'] = f"source_water_estep_{data['e_steps'][i]}"
#     return atoms

# def source_alanine_reader(file_path):
#     file_path = Path(file_path)
#     data = np.load(file_path, allow_pickle=True)
#     data = data.tolist()
#     # keys: 'atomic_number', 'pos', 'force', 'lengths', 'angles'
#     atoms = []
#     atoms = [AtomicConfiguration(numbers=data['atomic_number'],
#              positions=pos) for i, pos in enumerate(data['pos'])]
#     for i, atom in enumerate(atoms):
#         atom.info['forces'] = data['force'][i]
#         atom.info['lengths'] = data['lengths'][i]
#         atom.info['angles'] = data['angles']
#         atom.info['name'] = f"source_alanine_{i}"
#     return atoms

"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.5281/zenodo.5109599
Download link:
https://zenodo.org/record/5519311/files/zenodo_nep_version_2.zip?download=1

Unzip to project folder
unzip zenodo_nep_version_2.zip -d <project_dir>

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
Forces
Potential energy

Other properties added to metadata
----------------------------------
Virial (6-size vector)

File notes
----------
reader function uses energy.out files as targets to find
directories, then accesses respective virial, train and force
files from this directory.

"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import re
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/"
    "nep_prb_2021/zenodo_nep_version_2"
)
DATASET = "NEP_PRB_2021"
SOFT_METH = {
    "PbTe_Fan_2021": ("VASP", "DFT-PBE"),
    "Si_Fan_2021": ("CASTEP", "DFT-PW91"),
    "Silicene_Fan_2021": ("Quantum ESPRESSO", "DFT-PBE"),
}

LINKS = [
    "https://doi.org/10.5281/zenodo.5109599",
    "https://doi.org/10.1103/PhysRevB.104.104309",
]
AUTHORS = ["Zheyong Fan"]
DS_DESC = (
    "Approximately 7,000 distinct configurations of 2D-silicene, "
    "silicon, and PbTe. Silicon data used from "
    "http://dx.doi.org/10.1103/PhysRevX.8.041048. Dataset includes predicted "
    "force, potential energy and virial values."
)

E_UNITS = "eV"
F_UNITS = "eV/A"


CONF_RE = re.compile(
    r"^(?P<type>\S+)\s+(?P<x>\S+)\s+(?P<y>\S+)\s+"
    r"(?P<z>\S+)\s+(\S+)\s+(\S+)\s+(\S+)$"
)
CELL_RE = re.compile(
    r"(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)"
)

a_num_dict = {
    "Si": {"14": 14, "0": 14},
    "PbTe": {"52": 52, "82": 82, "0": 52, "1": 82},
    "Silicene": {"0": 14, "14": 14},
}


def read_en(filepath):
    with open(filepath, "r") as f:
        energies = [float(line.split()[0]) for line in f]

    return energies


def read_force(filepath, lens):
    line_no = 1
    conf_no = 0
    all_forces = []
    num_confs = len(lens)
    with open(filepath, "r") as f:
        c_len = lens[conf_no]
        forces = []
        for line in f:
            force_line = [float(i) for i in line.split()[:3]]
            forces.append(force_line)
            line_no += 1
            if line_no > c_len:
                all_forces.append(forces)
                line_no = 1
                conf_no += 1
                if conf_no > num_confs - 1:
                    return all_forces
                c_len = lens[conf_no]
                forces = []
    return all_forces


def read_virial(filepath):
    virials = []
    l_no = 1
    with open(filepath, "r") as f:
        vir = []
        for line in f:
            v = float(line.split()[0])
            vir.append(v)
            l_no += 1
            if l_no == 7:
                virials.append(vir)
                vir = []
                l_no = 1

    return virials


def read_train(filepath, a_num_dict):
    l_no = 1
    all_coords = []
    elements = []
    cells = []
    num_atoms = []
    with open(filepath, "r") as f:
        coords = []
        elems = []
        for line in f:
            # first line is number of configurations
            if l_no == 1:
                num_confs = int(line.rstrip())
                l_no += 1
            # following lines contain number of atoms for each configuration
            elif 1 < l_no <= num_confs + 1:
                num_atoms.append(int(line.split()[0]))
                l_no += 1
            elif l_no == num_confs + 2:
                l_no += 1
            # following lines contain coordinate and cell/lattice data
            # as well as training virial and force data that we are
            # not collecting(?)
            elif l_no > num_confs + 2:
                if any([line.startswith(x) for x in ["1 ", "0 ", "82 ", "52 ", "14 "]]):
                    match = CONF_RE.match(line)
                    elems.append(a_num_dict[match["type"]])
                    coords.append(
                        [
                            float(match["x"]),
                            float(match["y"]),
                            float(match["z"]),
                        ]
                    )
                    l_no += 1
                elif len(line.split()) == 9:
                    cells.append([float(x) for x in CELL_RE.match(line).groups()])
                    l_no += 1
                elif len(line.split()) == 7 or len(line.split()) == 1:
                    all_coords.append(coords)
                    elements.append(elems)
                    elems = []
                    coords = []
                    l_no += 1
                    pass
                else:
                    print("Something went wrong in coord parsing")
    elements.append(elems)
    all_coords.append(coords)
    cells = np.array(cells)
    cells = cells.reshape(cells.shape[0], 3, 3)
    return all_coords, cells, elements, num_atoms


def reader(filepath):
    """Uses the energy.out file as a target.
    Gathers force and virial from same parent directory
    """
    configs = []
    parent = filepath.parent
    dir_name = filepath.parts[-2]
    name = dir_name.split("_")[0]
    coords, cells, elements, num_atoms = read_train(
        parent / "train.in", a_num_dict[name]
    )
    forces = read_force(parent / "force.out", num_atoms)
    virials = read_virial(parent / "virial.out")
    energy = read_en(filepath)
    for i, coord in enumerate(coords):
        atom = AtomicConfiguration(positions=coord, numbers=elements[i], cell=cells[i])
        atom.info["name"] = f"{dir_name}_{i}"
        atom.info["forces"] = forces[i]
        atom.info["virials"] = virials[i]
        atom.info["energy"] = energy[i]
        atom.info["software"] = SOFT_METH[dir_name][0]
        atom.info["methods"] = SOFT_METH[dir_name][1]
        configs.append(atom)

    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["Si", "Pb", "Te"],
        reader=reader,
        glob_string="energy.out",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"field": "software"},
        "method": {"field": "methods"},
    }
    co_md_map = {
        "virials": {
            "field": "virials",
            "units": "eV/atom",
            "description": "A tensor of length 6 with virial stress values",
        },
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            co_md_map=co_md_map,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            f"{DATASET}-PbTe",
            "PbTe*",
            f"All PbTe configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-Si",
            "Si_*",
            f"All silicon configurations from {DATASET} dataset (excluding "
            "separate silicene set)",
        ],
        [
            f"{DATASET}-Silicene",
            "Silicene*",
            f"All Silicene configurations from {DATASET} dataset",
        ],
    ]

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
            cs_id = client.insert_configuration_set(co_ids, description=desc, name=name)

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

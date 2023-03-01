"""
author:gpwolfe

Zipped data can be downloaded from:
https://github.com/LarsSchaaf/reaction-barriers-mlffs

Unzip to project folder
unzip reaction-barriers-mlffs-main.zip "*.xyz" -d \
    $project_dir/scripts/cohinpt_schaaf

Change DATASET_FP to reflect location of data folder
Change database name as appropriate

Run: $ python3 <script_name> -i (or --ip) <database_ip>

Properties:
free energy
forces

Other properties added to metadata:
None

File notes
----------
A number of other properties are available, but meanings are
not all clear
"""


# .xyz header:
# Lattice="10.213697277208299 0.0 0.0 0.0 14.44429697782187 0.0 0.0 0.0 \
# 15.987396868621472" \
# Properties=species:S:1:pos:R:3:magmoms:R:1:qe3_forces:R:3\
#  free_energy=-92009.47191928465 dang-name=mono-HCOO_D \
# qe3_fenergy=-92009.47191928465 pbc="T T T"

# array keys = 'numbers', 'positions', 'magmoms', 'qe3_forces', 'forces', \
# 'local_gap_variance', 'momenta', 'local_gap_uncertainty'

# info keys = 'free_energy', 'qe3_fenergy', 'dang-name', 'energy', \
# 'time', 'name'
# free energy = energy != qe3_fenergy or qe4_fenergy

from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("scripts/cohinpt_schaaf/")
METHOD = "DFT-GAP"
SOFTWARE = "QuantumEspresso, CatKit"


def namer(info):
    if "doped" in info:
        return "Pt_doped_training"
    elif "barriers" in info:
        return "undoped_training"
    elif "active" in info:
        return "bi-HCOO_D"
    elif "dft-intermediates" in info:
        return info
    elif "figure-3" in info:
        return "dopant-configs"
    else:
        print("Something went wrong in naming.")


def reader(file_path):
    file_name = file_path.stem
    atoms = ase.io.read(file_path, index=":")
    for atom in atoms:
        atom.info["name"] = namer(file_name)
        atom.info["free_energy"] = atom.info.get(
            "free_energy", atom.info.get("qe4_fenergy")
        )
    return atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O", "C", "Pt", "In"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    pds = (atomic_forces_pd, free_energy_pd)
    for pd in pds:
        client.insert_property_definition(pd)
    names = set([atom.info["name"] for atom in configurations])
    metadata = {"software": {"value": SOFTWARE}, "method": {"value": METHOD}}
    property_map = {
        "free-energy": [
            {
                "energy": {"field": "free_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        # [
        #     "COHInPt_schaaf_2023",
        #     ".*",
        #     "All configurations in the COHInPt_schaaf_2023 set",
        # ],
        [
            "Pt-doped-training_COHInPt_schaaf_2023",
            "Pt_doped",
            "Pt-doped training data from COHInPt_schaaf_2023 set",
        ],
        [
            "Undoped-training_COHInPt_schaaf_2023",
            "undoped",
            "Non-Pt-doped training data from COHInPt_schaaf_2023 set",
        ],
    ]

    for name in names:
        cs_regexes.append(
            [
                f"COHInPt_schaaf_2023_{name}",
                name.replace("+", ".")
                .replace("-", ".")
                .replace("(", ".")
                .replace(")", "."),
                f"{name} molecular dynamics data from COHInPt_schaaf_2023 set",
            ]
        )

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="COHInPt_schaaf_2023",
        authors=["L. Schaaf, E. Fako, S. De, A. Schaefer, G. Csanyi"],
        links=[
            "https://github.com/LarsSchaaf/reaction-barriers-mlffs",
            "https://arxiv.org/pdf/2301.09931",
        ],
        description="Training and simulation data from machine learning "
        "force field model applied to steps of the hydrogenation of carbon"
        " dioxide to methanol over an indium oxide catalyst, with and"
        " without platinum doping.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

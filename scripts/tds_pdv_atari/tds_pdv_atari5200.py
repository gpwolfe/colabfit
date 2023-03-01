"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.5281/zenodo.7278341

File address:
https://zenodo.org/record/7278342/files/saidigroup/Metal-Oxide-Dataset-v1.0.zip?download=1

Unzip project directory
unzip Metal-Oxide-Dataset-v1.0.zip -d $project_dir/scripts/tds_pdv_atari
unzip "$project_dir/scripts/tds_pdv_atari/ \
    saidigroup-Metal-Oxide-Dataset-2de736d/*.zip" -C \
        $project_dir/scripts/tds_pdv_atari

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
virial

File notes
----------
"""
from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)

import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path("scripts/tds_pdv_atari")

name_dict = {
    "mp-1986_ZnO": ("Zn", "O"),
    "mp-2133_ZnO": ("Zn", "O"),
    "mp-2229_ZnO": ("Zn", "O"),
    "mp-8484_ZnO2": ("Zn", "O"),
    "thermal_expansion": ("Mg", "O"),
    "surface": ("Mg", "O"),
    "additional_structure": ("Mg", "O"),
    "bulk": ("Mg", "O"),
    "mp-617_PtO2": ("Pt", "O"),
    "mp-1285_PtO2": ("Pt", "O"),
    "mp-1604_Pt3O4": ("Pt", "O"),
    "mp-7868_PtO2": ("Pt", "O"),
    "mp-1077716_PtO2": ("Pt", "O"),
    "mp-353_Ag2O": ("Ag", "O"),
    "mp-499_AgO": ("Ag", "O"),
    "mp-1605_Ag3O4": ("Ag", "O"),
    "mp-1079720_AgO": ("Ag", "O"),
    "mp-361_Cu2O": ("Cu", "O"),
    "mp-1692_CuO": ("Cu", "O"),
    "mp-704645_CuO": ("Cu", "O"),
    "mp-1064456_CuO": ("Cu", "O"),
    "0_mp-1265_files": ("Mg", "O"),
}


# grab parent directory instead of actual file
def assemble_props(filepath: Path):
    prop_path = filepath.parent.glob("*.npy")
    props = {}
    for p in prop_path:
        key = p.stem
        props[key] = np.load(p)
    return props


def name_elem(filepath: Path):
    parts = filepath.parts[-9:]
    for key in name_dict:
        if key in parts:
            return key, name_dict[key]


def reader(filepath: Path):
    name, elements = name_elem(filepath)
    props = assemble_props(filepath)
    n_mol = props["coord"].shape[0]
    n_atoms = props["coord"].shape[1] // 3
    props["box"] = props["box"].reshape([n_mol, 3, 3])
    props["force"] = props["force"].reshape([n_mol, n_atoms, 3])
    props["coord"] = props["coord"].reshape([n_mol, n_atoms, 3])
    props["virial"] = props["virial"].reshape([n_mol, 3, 3])
    with open(next(filepath.parent.glob("type.raw"))) as f:
        atom_ix = [int(line.strip()) for line in f if line.strip() != ""]

    atoms = []
    for i, coord in enumerate(props["coord"]):
        atom = AtomicConfiguration(
            symbols=[elements[i] for i in atom_ix],
            positions=coord,
            cell=props["box"][i],
        )
        atom.info["force"] = props["force"][i]
        atom.info["virial"] = props["virial"][i]
        atom.info["energy"] = float(props["energy"][i])
        atom.info["name"] = name
        atoms.append(atom)
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
        elements=["Mg", "O", "Zn", "Pt", "Ag", "Cu"],
        reader=reader,
        glob_string="box.npy",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "VASP, LAMMPS"},
        "method": {"value": "DFT-DeePot-SE"},
        "virial": {"field": "virial"},
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
                "forces": {"field": "force", "units": "eV/A"},
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
        #     "TdS-PdV & Atari5200",
        #     ".*",
        #     "All configurations from TdS-PdV & Atari5200",
        # ],
        ["CuO", "Cu", "All Cu(x)O(y) configurations from TdS-PdV & Atari5200"],
        [
            "MgO",
            r"(0_mp\-1265_files)|(additional*)|(thermal*)|bulk|surface",
            "All Mg(x)O(y) configurations from TdS-PdV & Atari5200",
        ],
        ["ZnO", "Zn", "All Zn(x)O(y) configurations from TdS-PdV & Atari5200"],
        ["PtO", "Pt", "All Pt(x)O(y) configurations from TdS-PdV & Atari5200"],
        ["AgO", "Ag", "All Ag(x)O(y) configurations from TdS-PdV & Atari5200"],
    ]

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
        name="TdS-PdV_Atari5200",
        authors=["P Wisesa, C.M. Andolina, W.A. Saidi"],
        links=[
            "https://doi.org/10.5281/zenodo.7278341"
            "https://github.com/saidigroup/Metal-Oxide-Dataset/tree/v1.0",
            "https://doi.org/10.1021/acs.jpclett.2c03445",
        ],
        description="Approximately 45,000 configurations "
        "of metal oxides of Mg, Ag, Pt, Cu and Zn, with "
        "initial training structures taken from the "
        "Materials Project database.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

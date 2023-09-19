"""
author:gpwolfe

Properties
----------
energy
forces
virial

File notes
----------
Tested locally. Kubernetes files should  have same changes described below

the file for coordinates at:
23-Single-Element-DNPs-main/Training_Data/Zn/iter0/T225/T225_2/hxNPwQaO0Pg_2-2v8T_6JeSoclea/elastic/B222_dist03_0/set.000
is empty, so these will have to be ignored. I left the files in place but renamed the
.npy files to .npy_bad, which
will be ignored by the globbing function.

Some of the files don't get grouped properly by iteration upon untar, so these
are grouped manually
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets_2.0/saidi_23_dnps"
)  # HSRN K8s pod location
DATASET_FP = Path("data/23-Single-Element-DNPs-main/Training_Data")
DATASET = "23-DNPs-RSCDD-2023"

LINKS = [
    "https://doi.org/10.1039/D3DD00046J",
    "https://github.com/saidigroup/23-Single-Element-DNPs",
]
AUTHORS = ["Christopher M. Andolina", "Wissam A. Saidi"]
DS_DESC = (
    "One of 23 minimalist, curated sets of DFT-calculated properties for "
    "individual elements for the purpose of providing input to machine learning of "
    "atomic potentials. Each element set contains on average ~4000 structures with "
    "27 atoms per structure. Configuration metadata includes Materials Project ID "
    "where available, as well as temperatures at which MD trajectories were calculated."
    "These temperatures correspond to the melting temperature (MT) and 0.25*MT for "
    "elements with MT < 2000K, and MT, 0.6*MT and 0.25*MT for elements with MT > 2000K."
)
ELEMENTS = [
    "Ag",
    "Al",
    "Au",
    "Co",
    "Cu",
    "Ge",
    "I",
    "Kr",
    "Li",
    "Mg",
    "Mo",
    "Nb",
    "Ni",
    "Os",
    "Pb",
    "Pd",
    "Pt",
    "Re",
    "Sb",
    "Sr",
    "Ti",
    "Zn",
    "Zr",
]
GLOB_STR = "box.npy"
METHODS = "DFT-PBE"
SOFTWARE = "VASP"


def assemble_props(filepath: Path, element: str):
    props = {}
    prop_paths = list(filepath.parent.glob("*.npy"))
    for p in prop_paths:
        key = p.stem
        props[key] = np.load(p, allow_pickle=True)
    num_configs = props["force"].shape[0]
    num_atoms = props["force"].shape[1] // 3
    props["forces"] = props["force"].reshape(num_configs, num_atoms, 3)
    props["coord"] = props["coord"].reshape(num_configs, num_atoms, 3)
    props["box"] = props["box"].reshape(num_configs, 3, 3)
    virial = props.get("virial")
    if virial is not None:
        props["virial"] = virial.reshape(num_configs, 3, 3)
    props["symbols"] = [element for i in range(props["coord"].shape[1])]
    return props


def reader(filepath: Path):
    for elem in ELEMENTS:
        if elem in filepath.parts:
            element = elem
            break
    start_part = filepath.parts.index(element)
    name_parts = filepath.parts[start_part:-2]
    name = "_".join(name_parts)
    mp_id = None
    temp = None
    for part in name_parts:
        if part.isdigit():
            temp = int(part)
        if "_mp-" in part:
            mp_id = part.split("_")[1]

    props = assemble_props(filepath, element)
    configs = [
        AtomicConfiguration(
            symbols=props["symbols"], positions=pos, cell=props["box"][i]
        )
        for i, pos in enumerate(props["coord"])
    ]
    energy = props.get("energy")
    for i, c in enumerate(configs):
        c.info["forces"] = props["forces"][i]
        virial = props.get("virial")
        if virial is not None:
            c.info["virial"] = virial[i]
        # if energy is not None:
        c.info["energy"] = float(energy[i])
        if mp_id is not None:
            c.info["mp_id"] = mp_id
        c.info["name"] = f"{name}_{i}"
        if temp is not None:
            c.info["temp"] = temp
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "encut": {"value": "400 eV"},
    }
    co_md_map = {
        "materials-project-id": {"field": "mp_id"},
        "temperature": {"field": "temp"},
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
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    for element in ELEMENTS:
        ds_id = generate_ds_id()
        elem_fp = next(DATASET_FP.glob(element))
        configurations = load_data(
            file_path=elem_fp,
            file_format="folder",
            name_field="name",
            elements={element},
            reader=reader,
            glob_string=GLOB_STR,
            generator=False,
        )
        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=property_map,
                co_md_map=co_md_map,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))
        css = [
            (
                f"{DATASET}_{element}_initial",
                "_iter0_",
                f"Initial training configurations of {element} from {DATASET}",
            ),
            (
                f"{DATASET}_{element}_adaptive",
                "_iter[1-9]_",
                f"Adaptive training configurations of {element} from {DATASET}",
            ),
        ]
        cs_ids = []
        for name, reg, desc in css:
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                query={"names": {"$regex": reg}},
                name=name,
                description=desc,
                ds_id=ds_id,
            )
            cs_ids.append(cs_id)
        client.insert_dataset(
            do_hashes=all_do_ids,
            cs_ids=cs_ids,
            ds_id=ds_id,
            name=f"{DATASET}-{element}",
            authors=AUTHORS,
            links=LINKS,
            description=(
                f"Configurations of {element} from Andolina & Saidi, 2023. {DS_DESC}"
            ),
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

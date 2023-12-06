#!/usr/bin/env python
# coding: utf-8
"""
File notes
------------

"""
from argparse import ArgumentParser
import json
from pathlib import Path
import sys

import numpy as np

from pymatgen.core.structure import Structure
from pymatgen.io.ase import AseAtomsAdaptor

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    cauchy_stress_pd,
    atomic_forces_pd,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/data/mlearn")
DATASET_FP = Path.cwd().parent / "data/mlearn/mlearn-master/data"

PUBLICATION = "https://doi.org/10.1021/acs.jpca.9b08723"
DATA_LINK = "https://github.com/materialsvirtuallab/mlearn"
LINKS = [
    "https://doi.org/10.1021/acs.jpca.9b08723",
    "https://github.com/materialsvirtuallab/mlearn",
]
AUTHORS = [
    "Yunxing Zuo",
    "Chi Chen",
    "Xiangguo Li",
    "Zhi Deng",
    "Yiming Chen",
    "Jörg Behler",
    "Gábor Csányi",
    "Alexander V. Shapeev",
    "Aidan P. Thompson",
    "Mitchell A. Wood",
    "Shyue Ping Ong",
]


def reader(path):
    adaptor = AseAtomsAdaptor()

    with open(path, "r") as f:
        data = json.load(f)

        group_counts = {}

        for entry in data:
            struct = Structure.from_dict(entry["structure"])
            atoms = adaptor.get_atoms(struct)

            # Adding labels
            i = group_counts.get(entry["group"], 0)
            group_counts[entry["group"]] = i + 1

            atoms.info["_labels"] = entry["group"].lower()

            # Generating names
            clean_name = "_".join(entry["description"].split(" "))
            clean_name = clean_name.replace("/", "_")
            clean_name = clean_name.replace("(", "_")
            clean_name = clean_name.replace(")", "_")
            clean_name = clean_name.replace(",", "")

            clean_name = f'{entry["tag"]}_{clean_name}'

            # Loading DFT-computed values
            atoms.info["_name"] = [clean_name]

            atoms.info["per-atom"] = True
            atoms.info["energy"] = entry["outputs"]["energy"] / entry["num_atoms"]

            atoms.arrays["forces"] = np.array(entry["outputs"]["forces"])

            stress = np.zeros((3, 3))
            stress[0, 0] = entry["outputs"]["virial_stress"][0]
            stress[1, 1] = entry["outputs"]["virial_stress"][1]
            stress[2, 2] = entry["outputs"]["virial_stress"][2]
            stress[1, 2] = entry["outputs"]["virial_stress"][3]
            stress[0, 2] = entry["outputs"]["virial_stress"][4]
            stress[0, 1] = entry["outputs"]["virial_stress"][5]

            atoms.info["stress"] = stress

            # Add DFT settings

            if "Li" in atoms.get_chemical_symbols():
                kpoints = "3x3x3"
            else:
                kpoints = "4x4x4"

            atoms.info["input"] = {
                "kinetic-energy-cutoff": {"value": 520, "units": "eV"},
                "k-point-mesh": kpoints,
                "energy-convergence": {
                    "value": 1e-5,
                    "units": "eV",
                },
                "forces-convergence": {
                    "value": 0.02,
                    "units": "eV/Ang",
                },
            }

            yield AtomicConfiguration.from_ase(atoms)


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

    # TODO
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "input": {"field": "input"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "input": {"field": "input"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "kbar"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "input": {"field": "input"},
                },
            }
        ],
    }
    configuration_set_regexes = {
        "Ground|relaxed": "Ground state structure",
        "Vacancy": "NVT AIMD simulations of the bulk supercells with "
        "a single vacancy performed at 300 K and 2.0x of the melting point of each "
        "element. The bulk supercells were heated from 0 K to the target temperatures "
        "and equilibrated for 20,000 time steps. A total of 40 snapshots were obtained "
        "from the subsequent production run of each AIMD simulation at an interval of"
        " 0.1 ps.",
        "AIMD_NVT": "NVT ab initio molecular dynamics (AIMD) simulations of the bulk  "
        "supercells performed at 300 K and 0.5x, 0.9x, 1.5x, and "
        "2.0x of the melting point of each element with a time step of 2 fs. The bulk "
        "supercells were heated from 0 K to the target temperatures and equilibrated "
        "for 20 000 time steps. A total of 20 snapshots were obtained from the "
        "subsequent production run in each AIMD simulation at an interval of 0.1 ps.",
        "surface": "Slab structures up to a maximum Miller index of three, including "
        "(100), (110), (111), (210), (211), (310), (311), (320), (321), (322), "
        "(331), and (332), as obtained from the Crystalium database.",
        "strain": "Strained structures constructed by applying strains of -10% to "
        "10% at 2% intervals to the bulk supercell in six different modes. "
        "The supercells used are the 3 x 3 x 3, 3 x 3 x 3, and "
        "2 x 2 x 2 of the conventional bcc, fcc, and diamond unit cells, respectively",
    }
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    elements = [
        "Cu",
        "Ge",
        "Li",
        "Mo",
        "Ni",
        "Si",
    ]

    for elem in elements:
        images = load_data(
            file_path=DATASET_FP / elem,
            file_format="folder",
            name_field="_name",
            elements=[f"{elem}"],
            default_name="mlearn",
            reader=reader,
            glob_string="test.json",
            verbose=False,
        )

        ds_id = generate_ds_id()
        ids = client.insert_data(
            images, ds_id=ds_id, property_map=property_map, verbose=False
        )

        all_co_ids, all_pr_ids = list(zip(*ids))

        cs_ids = []
        cs_names = ["ground", "vacancy", "AIMD_NVT", "surface", "strain"]
        # TODO
        print(elem)
        for i, (regex, desc) in enumerate(configuration_set_regexes.items()):
            co_ids = client.get_data(
                "configurations",
                fields="hash",
                query={
                    "names": {"$regex": "test.*" + regex},
                    "elements": elem,
                },
                ravel=True,
            ).tolist()

            if co_ids:
                print(
                    f"\tConfiguration set {i}",
                    f"({regex}):".rjust(22),
                    f"{len(co_ids)}".rjust(7),
                )

                cs_id = client.insert_configuration_set(
                    co_ids, description=desc, ds_id=ds_id, name=f"{elem}_{cs_names[i]}"
                )

                cs_ids.append(cs_id)

        client.insert_dataset(
            cs_ids=cs_ids,
            ds_id=ds_id,
            do_hashes=all_pr_ids,
            name="mlearn_" + elem + "_test",
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=(
                "A comprehensive DFT data set was generated for six "
                "elements - Li, Mo, Ni, Cu, Si, and Ge. These elements "
                "were chosen to span a variety of chemistries (main group "
                "metal, transition metal, and semiconductor), crystal "
                "structures (bcc, fcc, and diamond) and bonding types "
                f"(metallic and covalent). This dataset comprises only the {elem} "
                "configurations"
            ),
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

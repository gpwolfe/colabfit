#!/usr/bin/env python
# coding: utf-8
"""
File notes
------------
This script divides the mlearn test dataset into different datasets, rather
than different configurations sets, based on elements included. Should probably
reconfigure to use configurations sets and, if necessary, the new query-upload
function
"""
from argparse import ArgumentParser
import glob
import itertools
import json
from pathlib import Path
import sys

import numpy as np

from pymatgen.core.structure import Structure
from pymatgen.io.ase import AseAtomsAdaptor

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data


DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/data/mlearn")
LINKS = [
    "https://doi.org/10.1021/acs.jpca.9b08723",
    "https://doi.org/10.48550/arXiv.1906.08888",
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
            atoms.info["ke_cutoff"] = 520  # eV

            if "Li" in atoms.get_chemical_symbols():
                atoms.info["k-point-mesh"] = "3x3x3"
            else:
                atoms.info["k-point-mesh"] = "4x4x4"

            atoms.info["energy-convergence"] = 1e-5  # eV
            atoms.info["forces-convergence"] = 0.02  # eV/Ang

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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    pds = glob.glob("/home/ubuntu/calc_notebooks/*.json")
    for pd in pds:
        client.insert_property_definition(pd)

    images = list(
        load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="_name",
            elements=[
                "Cu",
                "Ge",
                "Li",
                "Mo",
                "Ni",
                "Si",
            ],
            default_name="mlearn",  # default name with `name_field` not found
            reader=reader,
            glob_string="*test.json",
            verbose=True,
        )
    )

    set(itertools.chain.from_iterable([a.info["_labels"] for a in images]))

    # TODO
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "kinetic-energy-cutoff": {"field": "ke_cutoff", "units": "eV"},
                    "k-point-mesh": {"field": "k-point-mesh", "units": None},
                    "energy-convergence": {
                        "field": "energy-convergence",
                        "units": "eV",
                    },
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "kinetic-energy-cutoff": {"field": "ke_cutoff", "units": "eV"},
                    "k-point-mesh": {"field": "k-point-mesh", "units": None},
                    "forces-convergence": {
                        "field": "forces-convergence",
                        "units": "eV/Ang",
                    },
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "kilobar"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "kinetic-energy-cutoff": {"field": "ke_cutoff", "units": "eV"},
                    "k-point-mesh": {"field": "k-point-mesh", "units": None},
                },
            }
        ],
    }

    ids = client.insert_data(images, property_map=property_map, verbose=True)

    all_co_ids, all_pr_ids = list(zip(*ids))

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

    images[1]

    images[1].info

    cs_ids = {k: [] for k in ["Cu", "Ge", "Li", "Mo", "Ni", "Si"]}
    cs_names = ["ground", "vacancy", "AIMD_NVT", "surface", "strain"]
    # TODO
    co_ids_recheck = []
    for elem in cs_ids.keys():
        print(elem)
        count = 0
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
                co_ids_recheck += co_ids

                print(
                    f"\tConfiguration set {i}",
                    f"({regex}):".rjust(22),
                    f"{len(co_ids)}".rjust(7),
                )

                cs_id = client.insert_configuration_set(
                    co_ids, description=desc, name=f"{elem}_{cs_names[i]}"
                )

                cs_ids[elem].append(cs_id)

                count += len(co_ids)

    for elem, e_cs_ids in cs_ids.items():
        client.insert_dataset(
            cs_ids=e_cs_ids,
            do_hashes=all_pr_ids,
            name="mlearn_" + elem + "_test",
            authors=AUTHORS,
            links=LINKS,
            description=(
                "A comprehensive DFT data set was generated for six "
                "elements - Li, Mo, Ni, Cu, Si, and Ge. These elements "
                "were chosen to span a variety of chemistries (main group "
                "metal, transition metal, and semiconductor), crystal "
                "structures (bcc, fcc, and diamond) and bonding types "
                "(metallic and covalent). This dataset comprises only the {} "
                "configurations".format(elem)
            ),
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

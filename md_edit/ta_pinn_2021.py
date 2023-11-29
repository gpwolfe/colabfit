#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

import numpy as np

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/data/mishin/")
DATASET_FP = Path().cwd().parent / "data/mishin"
DATASET = "Ta_PINN_2021"

PUBLICATION = "https://doi.org/10.1016/j.commatsci.2021.111180"
DATA_LINK = "https://doi.org/10.1016/j.commatsci.2021.111180"
LINKS = ["https://doi.org/10.1016/j.commatsci.2021.111180"]
AUTHORS = ["Yi-Shen Lin", "Ganga P. Purja Pun", "Yuri Mishin"]
DS_DESC = (
    "A dataset consisting of the energies of supercells "
    "containing from 1 to 250 atoms. The supercells represent energy-volume relations "
    "for 8 crystal structures of Ta, 5 uniform deformation paths between pairs of "
    "structures, vacancies, interstitials, surfaces with low-index orientations, 4 "
    "symmetrical tilt grain boundaries, γ-surfaces on the (110) and (211) fault "
    "planes, a [111] screw dislocation, liquid Ta, and several isolated clusters "
    "containing from 2 to 51 atoms. Some of the supercells contain static atomic "
    "configurations. However, most are snapshots of ab initio MD simulations at "
    "different densities, and temperatures ranging from 293 K to 3300 K. The BCC "
    "structure was sampled in the greatest detail, including a wide range of "
    "isotropic and uniaxial deformations."
)

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
                "method": {"value": "DFT-PBE"},
                "ismear": {"value": 1},
                "sigma": {"value": 0.1},
                "k-point-scheme": {"value": "Monkhorst-Pack"},
                "encut": {"value": 410},
            },
        }
    ],
}


def reader(file_path):
    with open(file_path, "r") as infile:
        while infile:
            header = infile.readline()

            if header == "":
                break

            name = header.split("#")[0].strip()

            _ = infile.readline()  # scaling factor; always 1

            cell = np.array(
                [
                    [float(_.strip()) for _ in infile.readline().split()],
                    [float(_.strip()) for _ in infile.readline().split()],
                    [float(_.strip()) for _ in infile.readline().split()],
                ]
            )

            natoms = int(infile.readline().split()[0])

            _ = infile.readline()  # coordinates type; always 'C' for Cartesian

            positions = []
            # forces = []
            symbols = []
            for _ in range(natoms):
                x, y, z, fx, fy, fz, s = infile.readline().split()

                positions.append([float(x), float(y), float(z)])
                #                 forces.append([float(fx), float(fy), float(fz)])
                symbols.append(s)

            energy = float(infile.readline().split()[0])

            # Assuming periodic in all directions
            c = AtomicConfiguration(
                symbols=symbols, cell=cell, positions=np.array(positions), pbc=True
            )

            c.info["_name"] = name

            c.info["energy"] = energy
            #             c.arrays['forces'] = np.array(forces)

            c.info["per-atom"] = False

            yield c


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

    configurations = list(
        load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="_name",
            elements=["Ta"],
            default_name="Ta_PINN_2021",
            reader=reader,
            glob_string="*.dat",
            generator=False,
            verbose=True,
        )
    )

    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition('atomic-forces.json')
    # client.insert_property_definition('cauchy-stress.json')
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    cs_regexes = {
        "bcc.small.strain": "BCC structures with small homogeneous strains",
        "eos_A15": "A15 structures with isotropic strains at 0K",
        "eos_bcc": "BCC structures with isotropic strains at 0K",
        "eos_beta-Ta-shifted": "beta-Ta structures with isotropic strains at 0K",
        "eos_diamond": "Diamond structures with isotropic strains at 0K",
        "eos_fcc": "FCC structures with isotropic strains at 0K",
        "eos_hcp": "HCP structures with isotropic strains at 0K",
        "eos_hex": "Simple hexagonal structures with isotropic strains at 0K",
        "eos_sc": "Simple cubic structures with isotropic strains at 0K",
        "eos_dimer": "Dimer structures with isotropic strains at 0K",
        "eos_trimer-linear": "Linear trimer structures with isotropic strains at 0K",
        "eos_trimer-triangle": "Triangular trimer structures with isotropic strains "
        "at 0K",
        "eos_tetrahedron": "Tetrahedron structures with isotropic strains at 0K",
        "twinpath": "Samples along the twinning-antitwinning deformation path",
        "defpath_hex": "Samples along the hexagonal deformation path from "
        "BCC to HCP",
        "defpath_ortho": "Samples along the orthorhombic deformation path from BCC "
        "to BCT",
        "defpath_tetra": "Samples along the tetragonal deformation path from BCC "
        "to FCC to BCT",
        "defpath_trigo": "Samples along the trigonal deformation path from BCC to "
        "SC to FCC",
        "gamma_110": "Samples of the gamma surface in the (110) plane",
        "gamma_112": "Samples of the gamma surface in the (112) plane",
        "liquid": "NVT-MD snapshots of liquid structures at various temperatures",
        "bcc.nvt": "NVT-MD snapshots of BCC structures at various temperatures",
        "shockwave_100": "BCC structures with large uniaxial strain along the [100] "
        "direction",
        "shockwave_110": "BCC structures with large uniaxial strain along the [110] "
        "direction",
        "shockwave_111": "BCC structures with large uniaxial strain along the [111] "
        "direction",
        "dislocation": "NEB images from a dislocation relaxation run",
        "cluster": "Spherical clusters up to 2nd-/3rd-/4th- nearest-neighbor distances",
        "gb_111": "Sigma3 (111) grain boundary structures",
        "gb_112": "Sigma3 (112) grain boundary structures",
        "gb_210": "Sigma5 (210) grain boundary structures",
        "gb_310": "Sigma5 (310) grain boundary structures",
        "vacancy": "Vacancy configurations from NVT-MD and NEB calculations",
        "interstitial": "Interstitial configurations from NVT-MD and NEB calculations",
        "surface": "Relaxed (100), (110), and (111) surface structures, plus NVT-MD "
        "samples at 2500 K",
    }

    cs_names = [
        "bcc_small_strain",
        "eos_A15",
        "eos_bcc",
        "eos_beta_Ta_shifted",
        "eos_diamond",
        "eos_fcc",
        "eos_hcp",
        "eos_hex",
        "eos_sc",
        "eos_dimer",
        "eos_trimer_linear",
        "eos_trimer_triangle",
        "eos_tetrahedron",
        "twinpath",
        "defpath_hex",
        "defpath_ortho",
        "defpath_tetra",
        "defpath_trigo",
        "gamma_110",
        "gamma_112",
        "liquid",
        "bcc_nvt",
        "shockwave_100",
        "shockwave_110",
        "shockwave_111",
        "dislocation",
        "cluster",
        "gb_111",
        "gb_112",
        "gb_210",
        "gb_310",
        "vacancy",
        "interstitial",
        "surface",
    ]
    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=cs_names[i],
            ds_id=ds_id,
            description=desc,
            query={"names": {"$regex": regex}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


"""
configuration_label_regexes = {
    "bcc.small.strain": ["bcc", "strain"],
    "eos_A15": ["a15", "strain", "eos"],
    "eos_bcc": ["bcc", "strain", "eos"],
    "eos_beta-Ta-shifted": ["beta-ta", "strain", "eos"],
    "eos_diamond": ["diamond", "strain", "eos"],
    "eos_fcc": ["fcc", "strain", "eos"],
    "eos_hcp": ["hcp", "strain", "eos"],
    "eos_hex": ["sh", "strain", "eos"],
    "eos_sc": ["sc", "strain", "eos"],
    "eos_dimer": ["dimer", "strain", "eos"],
    "eos_trimer-linear": ["trimer", "strain", "eos"],
    "eos_trimer-triangle": ["trimer", "strain", "eos"],
    "eos_tetrahedron": ["tetrahedron", "strain", "eos"],
    "twinpath": ["twinning", "anti-twinning", "deformation_path"],
    "defpath_hex": ["deformation_path"],
    "defpath_ortho": ["deformation_path"],
    "defpath_tetra": ["deformation_path"],
    "defpath_trigo": ["deformation_path"],
    "gamma_110": ["gamma_surface"],
    "gamma_112": ["gamma_surface"],
    "liquid": ["md", "liquid", "nvt"],
    "bcc.nvt": ["md", "bcc", "nvt"],
    "shockwave_100": ["bcc", "strain"],
    "shockwave_110": ["bcc", "strain"],
    "shockwave_111": ["bcc", "strain"],
    "dislocation": ["dislocation"],
    "cluster": ["cluster"],
    "gb_111": ["grain_boundary", "sigma3"],
    "gb_112": ["grain_boundary", "sigma3"],
    "gb_210": ["grain_boundary", "sigma5"],
    "gb_310": ["grain_boundary", "sigma5"],
    "vacancy": ["vacancy", "nvt", "md"],
    "interstitial": ["interstitial", "nvt", "md"],
    "surface": ["surface", "nvt", "md"],
}

for regex, labels in configuration_label_regexes.items():
    client.apply_labels(
        dataset_id=ds_id,
        collection_name='configurations',
        query={'names': {'$regex': regex}},
        labels=labels,
        verbose=True
    )
"""


if __name__ == "__main__":
    main(sys.argv[1:])

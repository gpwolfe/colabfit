#!/usr/bin/env python
# coding: utf-8
"""
File notes:

Uses atomization-energy property imported from original script directory
"""

from argparse import ArgumentParser
from pathlib import Path
import sys
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets")

DATASET_NAME = "BA10-18"
LINKS = [
    "https://www.nature.com/articles/s41524-019-0189-9",
    "https://qmml.org/datasets.html",
]
AUTHORS = [
    "Chandramouli Nyshadham",
    "Matthias Rupp",
    "Brayden Bekker",
    "Alexander V. Shapeev",
    "Tim Mueller",
    "Conrad W. Rosenbrock",
    "Gábor Csányi",
    "David W. Wingate",
    "Gus L. W. Hart",
]
DESCRIPTION = "Dataset (DFT-10B) contains structures of the 10 binary alloys\
 AgCu, AlFe, AlMg, AlNi, AlTi, CoNi, CuFe, CuNi, FeV, and NbNi. Each\
 alloy system includes all possible unit cells with 1-8 atoms for\
 face-centered cubic (fcc) and body-centered cubic (bcc) crystal types,\
 and all possible unit cells with 2-8 atoms for the hexagonal close-packed\
 (hcp) crystal type. This results in 631 fcc, 631 bcc, and 333 hcp structures,\
 yielding 1595 x 10 = 15,950 unrelaxed structures in total. Lattice parameters\
 for each crystal structure were set according to Vegard's law. Total energies\
 were computed using DFT with projector-augmented wave (PAW) potentials within\
 the generalized gradient approximation (GGA) of Perdew, Burke, and Ernzerhof\
 (PBE) as implemented in the Vienna Ab Initio Simulation Package (VASP). The\
 k-point meshes for sampling the Brillouin zone were constructed using\
 generalized regular grids."


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

    configurations = load_data(
        file_path=DATASET_FP / "BA10-18/ba10-18-reformatted.xyz",
        file_format="xyz",
        name_field="name",
        elements=["Ag", "Cu", "Al", "Fe", "Mg", "Ni", "Ti", "Co", "V", "Nb"],
        default_name="BA10-18",
        verbose=True,
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)

    # client.insert_property_definition('/home/ubuntu/calc_notebook/atomization-energy.json')

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "te", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "kpoint": {"value": "generalized regular grids"},
                },
            }
        ],
        # Energies were computed at density functional level of
        # theory using projector-augmented wave potentials and
        # the PBE generalized gradient approximation functional.
        # k-point meshes constructed using generalized regular grids.
        # Method:
        # Total energies were computed using DFT with projector-augmented
        # wave (PAW) potentials within the generalized gradient approximation
        #  (GGA) of Perdew, Burke, and Ernzerhof (PBE) as implemented in
        # the Vienna Ab Initio Simulation Package (VASP)
        "atomization-energy": [
            {
                "energy": {"field": "fe", "units": "eV"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    # 'kpoint':{'value':'generalized regular grids'},
                },
            }
        ],
    }

    def tform(c):
        c.info["per-atom"] = False

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            co_md_map={"lattice_type": {"field": "lattice"}},
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))
    print(len(all_co_ids), len(list(set(all_co_ids))))

    # matches to data CO "name" field
    cs_regexes = {
        "AgCu": "Configurations of all AgCu structures",
        "AlFe": "Configurations of all AlFe structures",
        "AlMg": "Configurations of all AlMg structures",
        "AlNi": "Configurations of all AlNi structures",
        "AlTi": "Configurations of all AiTi structures",
        "CoNi": "Configurations of all CoNi structures",
        "CuFe": "Configurations of all CuFe structures",
        "CuNi": "Configurations of all CuNi structures",
        "FeV": "Configurations of all FeV structures",
        "NbNi": "Configurations of all NbNi structures",
    }

    cs_names = [
        "AgCu",
        "AlFe",
        "AlMg",
        "AlNi",
        "AlTi",
        "CoNi",
        "CuFe",
        "CuNi",
        "FeV",
        "NbNi",
    ]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({regex}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        cs_id = client.insert_configuration_set(
            co_ids, description=desc, name=cs_names[i]
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DESCRIPTION,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

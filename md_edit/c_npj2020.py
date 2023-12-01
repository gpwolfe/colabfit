#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
import ase


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/"
    "new_raw_datasets/C_allotropes_MingjianEllad/carbon_energies_forces/"
)
DATASET_FP = Path().cwd().parent / ""
DATASET = "C_NPJ2020"
PUBLICATION = "https://doi.org/10.1038/s41524-020-00390-8"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.12649811.v1"

LINKS = [
    "https://doi.org/10.1038/s41524-020-00390-8",
    "https://doi.org/10.6084/m9.figshare.12649811.v1",
]
AUTHORS = ["Mingjian Wen", "Ellad B. Tadmor"]
DS_DESC = (
    "The dataset consists of energies and forces for monolayer "
    "graphene, bilayer graphene, graphite, and diamond in various "
    "states, including strained static structures and configurations "
    "drawn from ab initio MD trajectories. A total number of 4788 "
    "configurations was generated from DFT calculations using the "
    "Vienna Ab initio Simulation Package (VASP). The energies and forces "
    "are stored in the extended XYZ format. One file for each configuration."
)


def tform(c):
    c.info["per-atom"] = False


def reader(file_path):
    file_name = file_path.stem
    atom = ase.io.read(file_path)
    atom.info["name"] = file_name
    yield atom


PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE+MDB"},
    "input": {
        "value": {"encut": {"value": 500, "units": "eV"}},
    },
}


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C"],
        # default_name='name',
        reader=reader,
        glob_string="*.xyz",
        # idk if this is right I copied the format off of InP_JPCA2020.py <-Eric
        # changed from *.extxyz to *.xyz since all files in this case have a .xyz
        # extension
        verbose=False,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "Energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": PI_MD,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/Ang"},
                "_metadata": PI_MD
                # The dataset is generated from DFT calculations using the Vienna
                # Ab initio Simulation Package51. The exchange-correlation energy
                # of the electrons is treated within the generalized gradient
                # approximated functional of Perdew, Burke, and Ernzerhof (PBE)52.
                # To capture van der Waals effects (a crucial aspect of interlayer
                # interactions in bilayer graphene and graphite), the semiempirical
                # many-body dispersion (MBD) method53 is applied. MBD accurately
                # reproduces many results from more advanced calculations and
                # experiments54. For monolayer graphene, a vacuum of 30 Å in the
                # direction perpendicular to the plane is chosen to minimize the
                # interaction between periodic images (similar for bilayer
                # graphene). An energy cutoff of 500 eV is employed for the
                # plane wave basis, and reciprocal space is sampled using a
                # Γ-centered Monkhorst Pack55 grid. The number of grid points
                # is set to 16 × 16 × 1 for the smallest supercell in the dataset
                # (monolayer graphene with two atoms) and to 4 × 4 × 4 for the
                # largest supercell (diamond with 64 atoms). For other structures,
                #  the number of grid points is selected to ensure that the energy
                #  is converged.
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=False,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    cs_regexes = {
        "bilayer": "Bilayer graphene configurations",
        "diamond": "Diamond configurations",
        "graphite": "graphite configurations",
        "monolayer": "monolayer graphene configurations",
    }

    cs_names = [
        "bilayer_graphene",
        "diamond",
        "graphite",
        "monolayer_graphene",
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
            f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7)
        )

        cs_id = client.insert_configuration_set(
            co_ids, ds_id=ds_id, description=desc, name=cs_names[i]
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

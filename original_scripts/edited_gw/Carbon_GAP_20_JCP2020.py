#!/usr/bin/env python
# coding: utf-8
"""
File notes:
----------
There my be reason to go back and add a configuration set of the
training data, since this appears to be a subset of the total data
but obtainable from a separate file. --gpw
"""
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets"
    "/Carbon_GAP_20/Carbon_GAP_20/"
)
DATASET = "Carbon_GAP_JCP2020"


LINKS = [
    "https://doi.org/10.1063/5.0005084",
    "https://www.repository.cam.ac.uk/handle/1810/307452",
]
AUTHORS = [
    "Patrick Rowe",
    "Volker L. Deringer",
    "Piero Gasparotto",
    "Gábor Csányi",
    "Angelos Michaelides",
]
DS_DESC = (
    "All of the training data generated in the construction of GAP-20 "
    "(approximately 17,000 configurations). GAP-20 describes the properties "
    "of the bulk crystalline and amorphous phases, crystal surfaces, and defect "
    "structures with an accuracy approaching that of direct ab initio simulation, "
    "but at a significantly reduced cost. The final potential is fitted to reference "
    "data computed using the optB88-vdW density functional theory (DFT) functional."
)


def tform(c):
    c.info["per-atom"] = False


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
        file_path=DATASET_FP / "Carbon_Data_Set_Total.xyz",
        file_format="xyz",
        name_field="config_type",
        elements=["C"],
        default_name="total_set",
        verbose=True,
        generator=False,
    )

    # seems like training set is only a subset of total set, need to check later.'

    configurations += load_data(
        file_path=DATASET_FP / "Carbon_GAP_20_Training_Set.xyz",
        file_format="xyz",
        name_field="config_type",
        elements=["C"],
        default_name="training_set",
        verbose=True,
        generator=False,
    )

    cs_list = set()
    for c in configurations:
        cs_list.add(*c.info["_name"])

    # metadata related to kpoints kpoints_density cutoff nneightol, will add these later
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "method": {"value": "DFT-optB88-vdW"},
                    "kpoint": {"field": "kpoints"},
                    # 'k-points-density':{'field':'kpoints_density'},
                    # 'cutoff':{'field':'cutoff'},
                    "ecut": {"value": "600 eV"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/Ang"},
                "_metadata": {
                    "method": {"value": "DFT/optB88-vdW"},
                    "kpoint": {"field": "kpoints"},
                    # 'k-points-density':{'field':'kpoints_density'},
                    # 'cutoff':{'field':'cutoff'},
                    "ecut": {"value": "600 eV"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "method": {"value": "DFT-optB88-vdW"},
                    "kpoint": {"field": "kpoints"},
                    # 'k-points-density':{'field':'kpoints_density'},
                    # 'cutoff':{'field':'cutoff'},
                    "ecut": {"value": "600 eV"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

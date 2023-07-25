"""
File notes
-----------
find better name for dataset
double-check properties/metadata
Improve dataset description
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd

DATASET_FP = Path("/large_data/new_raw_datasets_2.0/methane/methane.extxyz")
DATASET_FP = Path("data/methane/methane.extxyz")
DS_NAME = "methane"
DS_DESC = (
    "This dataset provides a large number (7,732,488) configurations for a simple CH4 "
    "composition, that are generated in an almost completely unbiased fashion."
    "This dataset is ideal to benchmark structural representations and regression "
    "algorithms, verifying whether they allow reaching arbitrary accuracy in the data "
    "rich regime."
)
LINKS = [
    "https://doi.org/10.1103/PhysRevLett.125.166001",
    "https://doi.org/10.1063/5.0021116",
    "https://archive.materialscloud.org/record/2020.110",
]
AUTHORS = [
    "Sergey Pozdnyakov",
    "Michael Willatt",
    "Michele Ceriotti",
]


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "Hartrees"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "psi4"},
                "method": {"value": "DFT-PBE"},
                "basis": {"value": "cc-pvdz"},
                # 'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "Hartrees/Bohr"},
            "_metadata": {
                "software": {"value": "psi4"},
                "method": {"value": "DFT/PBE"},
                "basis": {"value": "cc-pvdz"},
            },
        }
    ],
}


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
    ds_id = generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    # Loads data, specify reader function if not "usual" file format
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="extxyz",
        name_field=None,
        elements=["C", "H"],
        default_name="methane",
        verbose=True,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    # matches to data CO "name" field
    # cs_regexes = {
    #    '.*':
    #        'Silica datasets.
    # }
    """
    cs_names=['all']
    for i in cs_list:
        cs_regexes[i]='Configurations with the %s structure.' %i
        cs_names.append(i)
    """
    # print (cs_regexes)

    # cs_ids = []

    # for i, (regex, desc) in enumerate(cs_regexes.items()):
    #     co_ids = client.get_data(
    #         "configurations",
    #         fields="hash",
    #         query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
    #         ravel=True,
    #     ).tolist()

    #     print(
    #       f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7)
    #     )

    #     cs_id = client.insert_configuration_set(
    #         co_ids, description=desc, name=cs_names[i]
    #     )

    #     cs_ids.append(cs_id)

    client.insert_dataset(
        # cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

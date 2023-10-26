"""

File notes
----------
Tested locally, will run on Kubernetes

.xyz files have been edited to have an extxyz style header for easier ingest.
Used following function:

def new_header(fp):
    with open(fp, "r") as f:
        text = f.readlines()
        new_text = []
        for line in text:
            l = line.strip().split()
            if len(l) == 1 and l[0] not in ["10", "24"]:
                new_text.append(
                    f"Properties=species:S:1:pos:R:3:forces:R:3 energy={l[0]}\n"
                )
            else:
                new_text.append(line)
    with open(f"{fp.parent / fp.stem}.extxyz", "w") as f:
        f.writelines(new_text)

"""

from argparse import ArgumentParser
from ase.io import read
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/new_raw_datasets_2.0/flexible_molecules/Datasets/"
)
# DATASET_FP = Path("data/flexible_molecules/Datasets")  # comment out--local testing
DS_NAME = "flexible_molecules_JCP2021"
PUBLICATION = "https://doi.org/10.1063/5.0038516"
# Data in supplementary materials
DATA_LINK = "https://doi.org/10.1063/5.0038516"
LINKS = [
    "https://doi.org/10.1063/5.0038516",
]
AUTHORS = [
    "Valentin Vassilev-Galindo",
    "Gregory Fonseca",
    "Igor Poltavsky",
    "Alexandre Tkatchenko",
]
DS_DESC = (
    "Configurations of azobenzene featuring a cis to trans thermal inversion "
    "through three channels: inversion, rotation, and rotation assisted by inversion; "
    "and configurations of glycine as a simpler comparison molecule. "
    "All calculations were performed in FHI-aims software using the "
    "Perdew-Burke-Ernzerhof (PBE) exchange-correlation functional with "
    "the Tkatchenko-Scheffler (TS) method to account for van der Waals (vdW) "
    "interactions. The azobenzene sets contain calculations from several different "
    "MD simulations, including two long simulations initialized at 300 K; short "
    "simulations (300 steps) initialized at 300 K and shorter (.5fs) timestep; four "
    "simulations, two starting from each of cis and trans isomer, at 750 K "
    "(initialized at 3000 K); and simulations at 50 K (initialized at 300 K). The "
    "glycine isomerization set was built using one MD simulation starting from each "
    "of two different minima. Initializatin and simulation temperature were 500 K."
)
PI_MD = {
    "software": {"value": "FHI-aims"},
    "method": {"value": "DFT-PBE"},
}


def reader(fp):
    name = fp.stem
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
    return configs


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

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": PI_MD,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/mol A"},
                "_metadata": PI_MD,
            },
        ],
    }

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=None,
        reader=reader,
        glob_string="*.extxyz",
        generator=False,
    )

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

    cs_info = [
        {
            "name": "Azobenzene_inversion",
            "reg": "Azobenzene_inversion",
            "description": "Configurations of Azobenzene relaxed through an inversion "
            "channel",
        },
        {
            "name": "Azobenzene_rotation_and_inversion",
            "reg": "Azobenzene_rotation_and_inversion",
            "description": "Configurations of Azobenzene relaxed through rotation "
            "assisted by inversion "
            "structure",
        },
        {
            "name": "Azobenzene_rotation",
            "reg": "Azobenzene_rotation_[0-9]",
            "description": "Configurations of Azobenzene relaxed through a rotation "
            "channel",
        },
        {
            "name": "Glycine",
            "reg": "Glycine",
            "description": "Configurations of Glycine starting from Ip and IIIp "
            "isomers",
        },
    ]

    cs_ids = []

    for i in cs_info:
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            query={"names": {"$regex": i["name"] + "_*"}},
            name=i["name"],
            description=i["description"],
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

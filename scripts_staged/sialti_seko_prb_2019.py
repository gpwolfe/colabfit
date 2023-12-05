"""
author: Gregory Wolfe

Properties
----------
forces
stress
potential energy

Other properties added to metadata
----------------------------------

File notes
----------


"""
from argparse import ArgumentParser
from pathlib import Path
import sys
from tqdm import tqdm

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/sialti_seko_prb_2019")
DATASET_NAME = "Si_Al_Ti_Seko_PRB_2019"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1103/PhysRevB.99.214108"
# data downloaded from custom link sent by Dr. Seko
DATA_LINK = "None"
OTHER_LINKS = [
    "https://doi.org/10.1063/5.0129045",
    "https://sekocha.github.io/",
]
LINKS = [
    "https://doi.org/10.1103/PhysRevB.99.214108",
    "https://doi.org/10.1063/5.0129045",
    "https://sekocha.github.io/",
]
AUTHORS = ["Atsuto Seko", "Atsushi Togo", "Isao Tanaka"]
DATASET_DESC = (
    "This dataset is compiled of 10,000 selected structures from the "
    "ICSD, divided into training and test sets. The dataset was generated "
    "for the purpose of training a MLIP with introduced high-order linearly "
    "independent rotational invariants up to the sixth order based on spherical "
    "harmonics. DFT calculations were carried out with VASP using the PBE "
    "cross-correlation functional and an energy cutoff of 400 eV."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "input": {
        "value": {
            "energy-cutoff": {"value": "400 eV"},
            "ediff": 10e-3,
            "ediffg": 10e-2
            # "basis-set": {"field": "basis_set"}
        }
    },
}
GLOB_STR = "vasprun.xml.12242"

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "GPa"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}


def get_config(path):
    name = "_".join(path.parts[-4:-1])
    data = read(path, index=":", format="vasp-xml")
    for config in data:
        config.info["forces"] = config.get_forces()
        config.info["energy"] = config.get_total_energy()

        config.info["stress"] = config.get_stress(voigt=False)
        config.info["name"] = name
        return config


def reader_train(fp):
    configs = []
    for path in tqdm(sorted(fp.parents[3].rglob("vasprun.xml.*"))):
        print(path)
        if path.parts[-3] == "test":
            pass
        else:
            configs.append(get_config(path))

    return configs


def reader_test(fp):
    configs = []
    for path in fp.parents[3].rglob("vasprun.xml.*"):
        if path.parts[-3] == "train":
            pass
        else:
            configs.append(get_config(path))
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    dss = (
        (
            "Si_Al_Ti_Seko_PRB_2019_train",
            reader_train,
            "Test sets from Si_Al_Ti_Seko_PRB_2019. ",
        ),
        (
            "Si_Al_Ti_Seko_PRB_2019_test",
            reader_test,
            "Training sets from Si_Al_Ti_Seko_PRB_2019. ",
        ),
    )

    for ds_name, reader, ds_desc in dss:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=GLOB_STR,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        cs_regexes = (
            [
                f"{ds_name}_aluminum",
                "Al_",
                f"Configurations of aluminum from {ds_name}",
            ],
            [
                f"{ds_name}_silicon",
                "Si_",
                f"Configurations of silicon from {ds_name}",
            ],
            [
                f"{ds_name}_titanium",
                "Ti_",
                f"Configurations of titanium from {ds_name}",
            ],
        )

        cs_ids = []

        for cs_name, cs_regex, cs_desc in cs_regexes:
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=cs_name,
                description=cs_desc,
                query={"names": {"$regex": cs_regex}},
            )

            cs_ids.append(cs_id)

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
            description=ds_desc + DATASET_DESC,
            verbose=True,
            cs_ids=cs_ids,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

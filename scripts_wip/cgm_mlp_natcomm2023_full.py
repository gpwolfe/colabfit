"""
author: Gregory Wolfe

Properties
----------
energy
virials
forces

Other properties added to metadata
----------------------------------
config_type

File notes
----------

the file Graphite_testing_Set.xyz has no energy, forces or virial information

"""


from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/CGM-MLP-main/training_datasets")
DATASET_NAME = "CGM-MLP_natcomm2023_screening"
LICENSE = "https://opensource.org/licenses/MIT"

PUBLICATION = "https://doi.org/10.1038/s41467-023-44525-z"
DATA_LINK = "https://github.com/sjtudizhang/CGM-MLP"
# OTHER_LINKS = []

AUTHORS = ["Di Zhang", "Peiyun Yi", "Xinmin Lai", "Linfa Peng", "Hao Li"]
DATASET_DESC = (
    "This dataset was one of the datasets used in training during "
    "the process of producing an active learning dataset for the purposes of "
    "exploring substrate-catalyzed deposition on metal surfaces such as Cu(111), "
    "Cr(110), Ti(001), and oxygen-contaminated Cu(111) as "
    "a means of controllable synthesis of carbon nanomaterials. The combined dataset "
    "includes structures from the Carbon_GAP_20 dataset and additional configurations "
    "of carbon clusters on a Cu(111) surface."
)
ELEMENTS = None

PI_METADATA = {
    "software": {"value": "CP2K"},
    "method": {"value": "DFT-PBE+D3"},
    "input": {
        "value": {
            "cutoff-energy": {"value": 300, "units": "Ry"},
            "relative-cutoff-energy": {"value": 60, "units": "Ry"},
            "scheme": "QUICKSTEP",
        }
    },
}


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
            "forces": {"field": "force", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/angstrom^3"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "config_type": {"field": "config_type"},
}
# ds_name, ds_desc, ds_fp, ds_glob
DSS = (
    (
        DATASET_NAME + "_Cr-C",
        "Training simulations from {DATASET_NAME} of carbon deposition on "
        "a Cr surface. {DATASET_DESC}",
        DATASET_FP / "CGM-MLP-Cr-C",
        "Training_Dataset_deposition.xyz",
    ),
    (
        DATASET_NAME + "_Cu-C",
        "Training simulations from {DATASET_NAME} of carbon deposition on "
        "a Cu surface. {DATASET_DESC}",
        DATASET_FP / "CGM-MLP-Cu-C",
        "Training_Dataset_deposition.xyz",
    ),
    (
        DATASET_NAME + "_CU-C",
        "Training simulations from {DATASET_NAME} of carbon on a Cu metal "
        "surface. {DATASET_DESC}",
        DATASET_FP / "CGM-MLP-Cu-C/",
        "Training_Dataset_metal_surface.xyz",
    ),
)


def reader(filepath: Path):
    configs = read(filepath, index=":")
    for i, config in enumerate(configs):
        config.info[
            "name"
        ] = f"{'_'.join(filepath.parts[:-2])}_{config.info['config_type']}_{i}"
        stress = config.info.get("dft_virial")
        if stress is not None:
            config.info["stress"] = config.info["dft_virial"].reshape(3, 3)

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
    for i, (ds_name, ds_desc, filename) in enumerate(DSS):
        print(filename)
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=filename,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=False,
            )
        )
        css = [
            [
                f"{DATASET_NAME}_deposition",
                {"names": {"$regex": "deposition"}},
                f"Configurations of C deposition in a {ds_name.split('_')[-1]} system "
                f"from {DATASET_NAME} dataset",
            ],
            [
                f"{DATASET_NAME}_metallic_surface",
                {"names": {"$regex": "metallic_surface"}},
                f"Configurations of C on a metallic surface in a "
                f"{ds_name.split('_')[-1]} system from {DATASET_NAME} dataset",
            ],
        ]
        all_co_ids, all_do_ids = list(zip(*ids))

        cs_ids = []
        for i, (name, query, desc) in enumerate(css):
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=name,
                description=desc,
                query=query,
            )

            cs_ids.append(cs_id)

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
            description=ds_desc,
            verbose=False,
            # cs_ids=cs_ids,  # remove line if no configuration sets to insert
            data_license=LICENSE,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

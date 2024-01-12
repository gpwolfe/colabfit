"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
forces
stress
energy

Other properties added to metadata
----------------------------------

File notes
----------
the file "pathway.xyz" has to be edited: "properties" --> "Properties" to enable
xyz reading.
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

# call database using its name
# means to start with fresh database

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets/agpd")
DATASET_FP = Path().cwd().parent / "data/agpd"
DATASET_NAME = "AgPd_NPJ_2021"
LICENSE = "https://opensource.org/licenses/MIT"
AUTHORS = [
    "Conrad W. Rosenbrock",
    "Konstantin Gubaev",
    "Alexander V. Shapeev",
    "Livia B. Pártay",
    "Noam Bernstein",
    "Gábor Csányi",
    "Gus L. W. Hart",
]
PUBLICATION = "https://doi.org/10.1038/s41524-020-00477-2"
DATA_LINK = "https://github.com/msg-byu/agpd"
LINKS = [
    "https://doi.org/10.1038/s41524-020-00477-2",
    "https://github.com/msg-byu/agpd",
]
DESCRIPTION = (
    "The dataset consists of energies, forces and virials for "
    "DFT-VASP-generated Ag-Pd systems. The data was used to fit "
    "an active learned dataset which was used to "
    "compare MTP- and SOAP-GAP-generated potentials."
)
ELEMENTS = None
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"DFT-PBE"},
    "input": {"value": {"prec": "Accurate", "ediff": "1e-8"}},
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

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)

    configurations = load_data(
        file_path=DATASET_FP / "agpd-master/data/bcc.xyz",
        file_format="extxyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="bcc",
        verbose=False,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd-master/data/fcc.xyz",
        file_format="extxyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="fcc",
        verbose=False,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd-master/data/pathway.xyz",
        file_format="extxyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="pathway",
        verbose=False,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd-master/data/relaxed.xyz",
        file_format="extxyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="relaxed",
        verbose=False,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd-master/data/unrelaxed.xyz",
        file_format="extxyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="unrelaxed",
        verbose=False,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "vasp_energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"DFT-PBE"}
                    # Both the MTP and GAP potentials were fitted to the same
                    # active-learned dataset, while a liquid dataset provided
                    # validation for energies, forces, and “virials”
                    # (volume-weighted coefficients of the stress tensor).
                    # Although only the active-learned dataset was used for
                    # building the models, there was some overlap between the
                    # seed configurations in the active-learned dataset and the
                    # configurations for which phonons are predicted (discussed
                    # later), both having their origin in enumerated42
                    # supercells.
                    # https://www.nature.com/articles/s41524-020-00477-2#Sec2
                    #
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "vasp_force", "units": "eV/angstrom"},
                "_metadata": PI_MD,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "vasp_virial", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"DFT-PBE"},
                },
            }
        ],
    }

    for c in configurations:
        c.info["per-atom"] = False
        if "vasp_virial" in c.info:
            # Stress/Virial will almost always be listed as 9 numbers.
            # However, we would like it formatted as a 3x3 array so transform
            c.info["vasp_virial"] = c.info["vasp_virial"].reshape((3, 3))
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            # transform=tform,
            verbose=False,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    # matches to data CO "name" field
    cs_regexes = {
        "bcc": "775 enumerated BCC configurations selected IID within the "
        "Cluster Expansion basis.",
        "fcc": "775 enumerated FCC configurations selected IID within the "
        "Cluster Expansion basis.",
        "pathway": "Contains the 11 images for the transition pathway "
        "discussed in the paper.",
        "unrelaxed": "Contains 65 unrelaxed enumerated structures that are "
        "seeds for the phonon calculations.",
        "relaxed": "Contains relaxed versions of respective unrelaxed structures",
    }

    cs_names = [
        f"{DATASET_NAME}_bcc",
        f"{DATASET_NAME}_fcc",
        f"{DATASET_NAME}_pathway",
        f"{DATASET_NAME}_unrelaxed",
        f"{DATASET_NAME}_relaxed",
    ]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=cs_names[i],
            description=desc,
            query={"names": {"$regex": regex}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DESCRIPTION,
        resync=True,
        data_license=LICENSE,
        verbose=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

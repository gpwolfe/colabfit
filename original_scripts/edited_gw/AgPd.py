#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

# call database using its name
# means to start with fresh database

DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets")
DATASET_NAME = "AgPd_npj2021"
AUTHORS = [
    "Conrad W. Rosenbrock",
    "Konstantin Gubaev",
    "Alexander V. Shapeev",
    "Livia B. Pártay",
    "Noam Bernstein",
    "Gábor Csányi",
    "Gus L. W. Hart",
]
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
ELEMENTS = ["Ag", "Pd"]


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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)

    configurations = load_data(
        file_path=DATASET_FP / "agpd/agpd-master/data/bcc_original.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="bcc",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd/agpd-master/data/fcc_original.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="fcc",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd/agpd-master/data/pathway.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="pathway",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd/agpd-master/data/relaxed_original.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="relaxed",
        verbose=True,
        generator=False,
    )
    configurations += load_data(
        file_path=DATASET_FP / "agpd/agpd-master/data/unrelaxed_original.xyz",
        file_format="xyz",
        name_field=None,
        elements=ELEMENTS,
        default_name="unrelaxed",
        verbose=True,
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
                "forces": {"field": "vasp_force", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"DFT-PBE"},
                },
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

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            # transform=tform,
            verbose=True,
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
        "relaxed": "Contains relaxed versions of respective unrelaxed" " structures",
    }

    cs_names = ["bcc", "fcc", "pathway", "unrelaxed", "relaxed"]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            name=cs_names[i],
            description=desc,
            query={"names": {"$regex": regex}},
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

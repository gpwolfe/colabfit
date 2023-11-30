"""
authors: Gregory Wolfe, Alexander Tao

File notes
----------
configuration types (sets):{'bulk_amo', 'bulk_cryst', 'quench', 'cluster', 'liquid'}

virials and cauchy stress are present, but 210 of the config stress values are 6-size
tensors instead of 9. For this reason, stress is being included in the CO metadata and
virial is being included as the property instances

info keys

'config_type',
 'energy',
 'free_energy',
 'simulation_state',
 'stress',
 'sub_simulation_type',
 'virials'

arrays keys

forces
"""


from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read


from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
    potential_energy_pd,
)

# DS_PATH = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/silica")
DS_PATH = Path().cwd().parent / "data/silica_npjcm_2022/database/"
GLOB_STR = "dataset.scan.2.xyz"
DS_NAME = "Silica_NPJCM_2022"
AUTHORS = ["Linus C. Erhard", "Jochen Rohrer", "Karsten Albe", "Volker L. Deringer"]

PUBLICATION = "https://doi.org/10.1038/s41524-022-00768-w"
DATA_LINK = "https://doi.org/10.5281/zenodo.6353683"
LINKS = [
    "https://doi.org/10.1038/s41524-022-00768-w",
    "https://doi.org/10.5281/zenodo.6353683",
]
DS_DESC = (
    "This dataset was created for the purpose of training an MLIP for silica (SiO2). "
    "For initial DFT computations, GPAW (in combination with ASE) was used with LDA, "
    "PBE and PBEsol functionals; and VASP with the SCAN functional. All calculations "
    "used the projector augmented-wave method. After comparison, it was found that "
    "SCAN performed best, and all values were recalculated using SCAN. An energy "
    "cut-off of 900 eV and a k-spacing of 0.23 Å-1 were used."
)
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "SCAN"},
    "incar": {"value": {"encut": 900, "kspacing": 0.23}},
}

PI_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "virials", "units": "kB"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD,
        },
        {
            "stress": {"field": "stress", "units": "kB"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_MD,
        },
    ],
}
CO_MD = {
    key: {"field": key}
    for key in [
        "config_type",
        "simulation_state",
        "sub_simulation_type",
    ]
}


def reader(fp):
    configs = read(fp, index=":")
    for config in configs:
        if config.info.get("virials") is not None:
            if len(config.info["virials"]) == 9:
                config.info["virials"] = config.info["virials"].reshape((3, 3))
            else:
                virials = config.info["virials"]
                config.info["virials"] = [
                    [virials[0], virials[5], virials[4]],
                    [virials[5], virials[1], virials[3]],
                    [virials[4], virials[3], virials[2]],
                ]
        if config.arrays.get("stress") is not None:
            config.info["stress"] = config.arrays.pop("stress")
        if config.info.get("stress") is not None:
            if config.info["stress"].shape != (3, 3):
                stress = config.info["stress"]
                config.info["stress"] = [
                    [stress[0], stress[5], stress[4]],
                    [stress[5], stress[1], stress[3]],
                    [stress[4], stress[3], stress[2]],
                ]
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

    ds_id = generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(free_energy_pd)

    configurations = load_data(
        file_path=DS_PATH,
        file_format="folder",
        name_field="config_type",
        elements=["Si", "O"],
        verbose=True,
        reader=reader,
        generator=False,
        glob_string=GLOB_STR,
    )

    ids = list(
        client.insert_data(
            ds_id=ds_id,
            configurations=configurations,
            co_md_map=CO_MD,
            property_map=PI_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    css = {
        (
            "bulk_amo",
            "SiO2_bulk_amorphous",
            "SiO2 snapshot taken after equilibration of the amorphous phase.",
        ),
        ("bulk_cryst", "SiO2_bulk_crystal", "SiO2 structures in crystalline phase"),
        (
            "quench",
            "SiO2_quench_phase",
            "SiO2 snapshots taken during the quenching process",
        ),
        (
            "cluster",
            "SiO2_small_cluster",
            "SiO2 dimers and configurations with added oxygen clusters",
        ),
        (
            "liquid",
            "SiO2_liquid_phase",
            "SiO2 snapshots taken after equilibrating the liquid phase",
        ),
    }
    cs_ids = []

    for i, (reg, name, desc) in enumerate(css):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": reg}},
        )
        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

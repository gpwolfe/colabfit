"""
author: Gregory Wolfe, Alexander Tao

File notes
----------
Lattice
Properties=species:S:1:pos:R:3:magnetic_moments:R:1:forces:R:3
virial
energy
stress
free_energy
"energy_our GAP"=-22.927362757204524
"binding_energy_our GAP"=-2.2510694511666527
"energy_GAP Dragoni"=-13838.48512957841
"binding_energy_GAP Dragoni"=1.1654634043225087
"energy_EAM Mendelev"=-7.652258547705728
"binding_energy_EAM Mendelev"=-1.913064636926432
energy_Finnis-Sinclair=-6.819798926783503
binding_energy_Finnis-Sinclair=-1.7049497316958757
pbc="T T T"


"""

from argparse import ArgumentParser
from ase.io import read
from pathlib import Path
import sys

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)

DS_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/Iron_nanoparticle/")
# DS_FP = Path().cwd().parent / "data/fe_nano"  # local
DS_NAME = "Fe_nanoparticles_PRB_2023"
AUTHORS = ["Richard Jana", "Miguel A. Caro"]

PUBLICATION = "https://doi.org/10.1103/PhysRevB.107.245421"
DATA_LINK = "https://doi.org/10.5281/zenodo.7632315"
LINKS = [
    "https://doi.org/10.1103/PhysRevB.107.245421",
    "https://doi.org/10.5281/zenodo.7632315",
]
DS_DESC = (
    "This iron nanoparticles database contains dimers; trimers; bcc, fcc, "
    "hexagonal close-packed (hcp), simple cubic, and diamond crystalline structures. "
    "A wide range of cell parameters, as well as rattled structures, bcc-fcc and "
    "bcc-hcp transitional structures, surface slabs cleaved from relaxed bulk "
    "structures, nanoparticles and liquid configurations are included. "
    "The energy, forces and virials for the atomic structures were computed at the "
    "DFT level of theory using VASP with the PBE functional and standard PAW "
    "pseudopotentials for Fe (with 8 valence electrons, 4s^23d^6). The kinetic "
    "energy cutoff for plane waves was set to 400 eV and the energy threshold for "
    "convergence was 10-7 eV. All the DFT calculations "
    "were carried out with spin polarization."
)
GLOB_STR = "*.xyz"

CO_MD = {
    key: {"field": key}
    for key in [
        "magnetic_moments",
        "energy_our GAP",
        "binding_energy_our GAP",
        "energy_GAP Dragoni",
        "binding_energy_GAP Dragoni",
        "energy_EAM Mendelev",
        "binding_energy_EAM Mendelev",
        "energy_Finnis-Sinclair",
        "binding_energy_Finnis-Sinclair",
    ]
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "method": {"value": "DFT-PBE"},
                "software": {"value": "VASP"},
                "ecut": {"value": "400 eV"},
            },
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/Ang"},
            "_metadata": {
                "method": {"value": "DFT-PBE"},
                "software": {"value": "VASP"},
                "ecut": {"value": "400 eV"},
            },
        }
    ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "method": {"value": "DFT-PBE"},
                "software": {"value": "VASP"},
                "ecut": {"value": "400 eV"},
            },
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "kB"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
            },
        },
        {
            "stress": {"field": "virial", "units": "kB"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": {
                "software": {"value": "VASP"},
            },
        },
    ],
}


def reader(fp):
    data = read(fp, index=":", format="extxyz")
    for i, config in enumerate(data):
        config.info["magnetic_moments"] = config.arrays["magnetic_moments"]
        config.info["name"] = f"fe_nano_{i}"
        for key in config.info.keys():
            if " " in key:
                key = key.replace(" ", "-")
    return data


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

    configurations = load_data(
        file_path=DS_FP,
        file_format="folder",
        name_field="name",
        elements=["Fe"],
        verbose=True,
        reader=reader,
        generator=False,
        glob_string=GLOB_STR,
    )

    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            co_md_map=CO_MD,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
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

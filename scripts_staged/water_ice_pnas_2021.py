"""
author: Gregory Wolfe

Properties
----------
energy
forces

Other properties added to metadata
----------------------------------

File notes
----------
uses n2p2 file format

"""
from argparse import ArgumentParser
from pathlib import Path
import re
import sys

# from ase.io import read

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/water-ice-group-simple-MLP-8bcaedd_pnas_2021/training-sets")
DATASET_NAME = "water_ice_PNAS_2021"
LICENSE = "Creative Commons Attribution-ShareAlike 4.0 International"

SOFTWARE = "CP2K"
PUBLICATION = "https://doi.org/10.1073/pnas.2110077118"
DATA_LINK = "https://doi.org/10.5281/zenodo.5235246"
OTHER_LINKS = ["https://github.com/water-ice-group/simple-MLP"]
LINKS = [
    "https://doi.org/10.1073/pnas.2110077118",
    "https://doi.org/10.5281/zenodo.5235246",
    "https://github.com/water-ice-group/simple-MLP",
]
AUTHORS = [
    "Christoph Schran",
    "Fabian L. Thiemann",
    "Patrick Rowe",
    "Erich A. Müller",
    "Ondrej Marsalek",
    "Angelos Michaelides",
]
DATASET_DESC = (
    "Dataset generated using a committee-based active learning strategy "
    "to build a training dataset for modeling complex aqueous systems."
)
ELEMENTS = None

# Assign additional relevant property instance metadata, such as basis set used
CSS = {
    "bnnt-h2o": {
        "name": "hexagonal_boron_nitride_nanotubes_water",
        "desc": "Simulations of water confined in hexagonal boron nitride nanotubes",
        "metadata": {
            "software": {"value": SOFTWARE},
            "method": {"value": "DFT-PBE-D3"},
            "energy-cutoff": {"value": "460 Ry"},
            "basis-set": {"value": "DZVP"},
        },
    },
    "cnt-h2o": {
        "name": "carbon_nitride_nanotubes_water",
        "desc": "Simulations of water confined in carbon nitride nanotubes",
        "metadata": {
            "software": {"value": SOFTWARE},
            "method": {"value": "DFT-PBE-D3"},
            "energy-cutoff": {"value": "460 Ry"},
            "basis-set": {"value": "DZVP"},
        },
    },
    "f-h2o": {
        "name": "fluoride_water",
        "desc": "Simulations of fluoride ions in water",
        "metadata": {
            "software": {"value": SOFTWARE},
            "method": {"value": "DFT-revPBE0-D3"},
            "energy-cutoff": {"value": "400 Ry"},
            "basis-set": {"value": "TZV2P"},
        },
    },
    "mos2-h2o": {
        "name": "molybdenum_disulphide_water",
        "desc": "Simulations of water confined within molybdenum disulphide",
        "metadata": {
            "software": {"value": SOFTWARE},
            "method": {"value": "DFT-optB88-vdW"},
            "energy-cutoff": {"value": "550 Ry"},
            "basis-set": {"value": "DZVP"},
        },
    },
    "so4-h2o": {
        "name": "sulphate_water",
        "desc": "Simulations of sulphate ions in water",
        "metadata": {
            "software": {"value": SOFTWARE},
            "method": {"value": "DFT-BLYP-D3"},
            "energy-cutoff": {"value": "280 Ry"},
            "basis-set": {"value": "TZV2P"},
        },
    },
    "tio2-h2o": {
        "name": "rutile_titaniom_dioxide_water",
        "desc": "Simulations of water on a rutile titanium dioxide surface",
        "metadata": {
            "software": {"value": SOFTWARE},
            "method": {"value": "DFT-optB88-vdW"},
            "energy-cutoff": {"value": "400 Ry"},
            "basis-set": {"value": "DZVP"},
        },
    },
}
ATOM_RE = re.compile(
    r"atom\s+(?P<x>\-?\d+\.\d+)\s+(?P<y>\-?\d+\.\d+)\s+"
    r"(?P<z>\-?\d+\.\d+)\s+(?P<element>\w{1,2})\s+0.0+\s+0.0+\s+(?P<f1>\-?\d+\.\d+)"
    r"\s+(?P<f2>\-?\d+\.\d+)\s+(?P<f3>\-?\d+\.\d+)"
)
LATT_RE = re.compile(
    r"lattice\s+(?P<lat1>\-?\d+\.\d+)\s+(?P<lat2>\-?\d+\.\d+)\s+(?P<lat3>\-?\d+\.\d+)"
)
EN_RE = re.compile(
    r"comment\s+i\s=\s+\d+,\s+time\s=\s+\-?\d+\.\d+,\s+E\s=\s+(?P<energy>\-?\d+\.\d+)"
)


def reader(filepath):
    with open(filepath) as f:
        configurations = []
        lattice = []
        coords = []
        forces = []
        elements = []
        counter = 0
        for line in f:
            if (
                line.startswith("begin")
                # or line.startswith("end")
                or line.startswith("charge")
                or line.startswith("energy")
            ):
                pass
            elif line.startswith("lattice"):
                lattice.append([float(x) for x in LATT_RE.match(line).groups()])
            elif line.startswith("atom"):
                ln_match = ATOM_RE.match(line)
                coords.append([float(x) for x in ln_match.groups()[:3]])
                forces.append([float(x) for x in ln_match.groups()[-3:]])
                elements.append(ln_match.groups()[3])
            elif line.startswith("comment"):
                energy = float(EN_RE.match(line).group("energy"))
            elif line.startswith("end"):
                config = AtomicConfiguration(
                    positions=coords, symbols=elements, cell=lattice
                )
                config.info["forces"] = forces
                config.info["energy"] = energy
                config.info["name"] = f"{filepath.parts[-2]}_{counter}"
                configurations.append(config)
                # if counter == 100:  # comment after testing
                #     return configurations  # comment after testing
                counter += 1
                lattice = []
                coords = []
                forces = []
                elements = []
    return configurations


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
    client.insert_property_definition(potential_energy_pd)

    ds_id = generate_ds_id()

    configurations = []
    cs_ids = []

    for key, val in CSS.items():
        configurations += load_data(
            file_path=DATASET_FP / key,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string="input.data",
            generator=False,
        )
        property_map = {
            "potential-energy": [
                {
                    "energy": {"field": "energy", "units": "eV"},
                    "per-atom": {"value": False, "units": None},
                    "_metadata": val["metadata"],
                }
            ],
            "atomic-forces": [
                {
                    "forces": {"field": "forces", "units": "eV/A"},
                    "_metadata": val["metadata"],
                },
            ],
        }
        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=val["name"],
            description=val["desc"],
            query={"names": {"$regex": key}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.24435/materialscloud:m7-50

Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces
stress

Other properties added to metadata
----------------------------------
Many other properties added, similar to data from Materials Project
database

File notes
----------
Dataset size is too large to run on local machine--insufficient memory
"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import json
from pathlib import Path
import sys

DATASET_FP = Path("data")
DATASET = "DCGAT"

SOFTWARE = "VASP"
METHODS = "DFT(PBE)"
LINKS = [
    "https://doi.org/10.24435/materialscloud:m7-50",
    "https://doi.org/10.1002/adma.202210788",
]
AUTHORS = [
    "Jonathan Schmidt",
    "Noah Hoffmann",
    "Hai-Chen Wang",
    "Pedro Borlido",
    "Pedro J. M. A. Carriço",
    "Tiago F. T. Cerqueira",
    "Silvana Botti",
    "Miguel A. L. Marques",
]
DS_DESC = "Approximately 2.3 million configurations from 3 datasets curate\
 for the purpose of training a crystal graph attention network machine\
 learning model. Includes structures gathered from the Materials Project and\
 AFLOW databases, among other sources."
ELEMENTS = [
    "Ac",
    "Ag",
    "Al",
    "Ar",
    "As",
    "Au",
    "B",
    "Ba",
    "Be",
    "Bi",
    "Br",
    "C",
    "Ca",
    "Cd",
    "Ce",
    "Cl",
    "Co",
    "Cr",
    "Cs",
    "Cu",
    "Dy",
    "Er",
    "Eu",
    "F",
    "Fe",
    "Ga",
    "Gd",
    "Ge",
    "H",
    "He",
    "Hf",
    "Hg",
    "Ho",
    "I",
    "In",
    "Ir",
    "K",
    "Kr",
    "La",
    "Li",
    "Lu",
    "Mg",
    "Mn",
    "Mo",
    "N",
    "Na",
    "Nb",
    "Nd",
    "Ne",
    "Ni",
    "Np",
    "O",
    "Os",
    "P",
    "Pa",
    "Pb",
    "Pd",
    "Pm",
    "Pr",
    "Pt",
    "Pu",
    "Rb",
    "Re",
    "Rh",
    "Ru",
    "S",
    "Sb",
    "Sc",
    "Se",
    "Si",
    "Sm",
    "Sn",
    "Sr",
    "Ta",
    "Tb",
    "Tc",
    "Te",
    "Th",
    "Ti",
    "Tl",
    "Tm",
    "U",
    "V",
    "W",
    "Xe",
    "Y",
    "Yb",
    "Zn",
    "Zr",
]

GLOB_STR = "*.json"


def reader(filepath):
    name = filepath.stem
    with open(filepath) as f:
        data = json.loads(f.read())
    configs = []
    for i, entry in enumerate(data["entries"]):
        cell = entry["structure"]["lattice"]["matrix"]
        elements = []
        occu = []
        abc = []
        positions = []
        magmom = []
        charge = []
        forces = []
        for site in entry["structure"]["sites"]:
            elements.append(site["species"][0]["element"])
            occu.append(site["species"][0]["occu"])
            abc.append(site["abc"])
            positions.append(site["xyz"])
            magmom.append(site["properties"]["magmom"])
            charge.append(site["properties"]["charge"])
            forces.append(site["properties"]["forces"])

        config = AtomicConfiguration(
            positions=positions,
            cell=cell,
            symbols=elements,
        )
        config.info["occu"] = occu
        config.info["abc"] = abc
        config.info["magmom"] = magmom
        config.info["charge"] = charge
        config.info["forces"] = forces
        config.info["elements"] = elements

        config.info["energy"] = entry["energy"]
        config.info["stress"] = [list(x) for x in (entry["data"]["stress"])]
        config.info["correction"] = entry["correction"]
        config.info["energy_adjustments"] = [
            x["value"] for x in entry["energy_adjustments"]
        ]
        config.info["mat_id"] = entry["data"]["mat_id"]

        config.info["prototype_id"] = entry["data"]["prototype_id"]
        config.info["spg"] = entry["data"]["spg"]

        config.info["energy_total"] = entry["data"]["energy_total"]
        config.info["total_mag"] = entry["data"]["total_mag"]
        config.info["band_gap_ind"] = entry["data"]["band_gap_ind"]
        config.info["band_gap_dir"] = entry["data"]["band_gap_dir"]
        config.info["dos_ef"] = entry["data"]["dos_ef"]
        config.info["energy_corrected"] = entry["data"]["energy_corrected"]
        config.info["e_above_hull"] = entry["data"]["e_above_hull"]
        config.info["e_form"] = entry["data"]["e_form"]
        config.info["e_phase_separation"] = entry["data"]["e_phase_separation"]
        config.info["decomposition"] = entry["data"]["decomposition"]

        config.info["charge"] = entry["structure"]["charge"]

        config.info["lattice_a"] = entry["structure"]["lattice"]["a"]
        config.info["lattice_b"] = entry["structure"]["lattice"]["b"]
        config.info["lattice_c"] = entry["structure"]["lattice"]["c"]
        config.info["lattice_alpha"] = entry["structure"]["lattice"]["alpha"]
        config.info["lattice_beta"] = entry["structure"]["lattice"]["beta"]
        config.info["lattice_gamma"] = entry["structure"]["lattice"]["gamma"]
        config.info["lattice_volume"] = entry["structure"]["lattice"]["volume"]
        config.info["name"] = f"{name}_{i}"

        configs.append(config)
    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(cauchy_stress_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        # "": {"field": ""}
    }

    keys_used = [
        k
        for k in [
            "occu",
            "abc",
            "magmom",
            "charge",
            "elements",
            "correction",
            "energy_adjustments",
            "mat_id",
            "prototype_id",
            "spg",
            "energy_total",
            "total_mag",
            "band_gap_ind",
            "band_gap_dir",
            "dos_ef",
            "energy_corrected",
            "e_above_hull",
            "e_form",
            "e_phase_separation",
            "decompositio",
            "lattice_a",
            "lattice_b",
            "lattice_c",
            "lattice_alpha",
            "lattice_beta",
            "lattice_gamma",
            "lattice_volume",
        ]
    ]
    metadata.update({k: {"field": k} for k in keys_used})
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            f"{DATASET}-DCGAT-1",
            "dcgat_1_*",
            f"All configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-DCGAT-2",
            "dcgat_2_*",
            f"All configurations from {DATASET} dataset",
        ],
        [
            f"{DATASET}-DCGAT-3",
            "dcgat_3_*",
            f"All configurations from {DATASET} dataset",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={
                "hash": {"$in": all_co_ids},
                "names": {"$regex": regex},
            },
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )
        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)
        else:
            pass

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
        cs_ids=cs_ids,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

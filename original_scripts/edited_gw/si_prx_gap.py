#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase
from colabfit.tools.database import load_data


DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/data/gap_si/gp_iter6_sparse9k.xml.xyz"
)
DATASET = "Si_PRX_GAP"


LINKS = [
    "https://doi.org/10.1103/PhysRevX.8.041048",
    "https://doi.org/10.17863/CAM.65004",
]
AUTHORS = ["Albert P. Bartók", "James Kermode", "Noam Bernstein", "Gábor Csányi"]
DS_DESC = (
    "The original DFT training data for the general-purpose silicon "
    "interatomic potential described in the associated publication. "
    "The kinds of configuration that we include are chosen using "
    "intuition and past experience to guide what needs to be included "
    "to obtain good coverage pertaining to a range of properties."
)


def tform(img):
    img.info["per-atom"] = False
    # Renaming some fields to be consistent
    info_items = list(img.info.items())
    for key, v in info_items:
        if key in ["_name", "_labels", "_constraints"]:
            continue

        del img.info[key]
        img.info[key.replace("_", "-").lower()] = v
    arrays_items = list(img.arrays.items())
    for key, v in arrays_items:
        del img.arrays[key]
        img.arrays[key.replace("_", "-").lower()] = v
    # Converting some string values to floats
    for k in [
        "md-temperature",
        "md-cell-t",
        "smearing-width",
        "md-delta-t",
        "md-ion-t",
        "cut-off-energy",
        "elec-energy-tol",
    ]:
        if k in img.info:
            try:
                img.info[k] = float(img.info[k].split(" ")[0])
            except:  # noqa: E722
                pass
    # Reshaping shape (9,) stress vector to (3, 3) to match definition
    if "dft-virial" in img.info:
        img.info["dft-virial"] = img.info["dft-virial"].reshape((3, 3))

    if "gap-virial" in img.info:
        img.info["gap-virial"] = img.info["gap-virial"].reshape((3, 3))


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

    images = list(
        load_data(
            file_path=DATASET_FP,
            file_format="xyz",
            name_field="config_type",
            elements=["Si"],
            default_name="Si_PRX_GAP",
            verbose=True,
        )
    )

    settings_keys = [
        "mix-history-length",
        "castep-file-name",
        "grid-scale",
        "popn-calculate",
        "n-neighb",
        "oldpos",
        "i-step",
        "md-temperature",
        # 'positions',
        "task",
        "data-distribution",
        "avg-ke",
        "force-nlpot",
        "continuation",
        "castep-run-time",
        "calculate-stress",
        "minim-hydrostatic-strain",
        "avgpos",
        "frac-pos",
        "hamiltonian",
        "md-cell-t",
        "cutoff-factor",
        "momenta",
        "elec-energy-tol",
        "mixing-scheme",
        "minim-lattice-fix",
        "in-file",
        "travel",
        "thermostat-region",
        "time",
        "temperature",
        "kpoints-mp-grid",
        "cutoff",
        "xc-functional",
        "smearing-width",
        "pressure",
        "reuse",
        "fix-occupancy",
        "map-shift",
        "md-num-iter",
        "damp-mask",
        "opt-strategy",
        "spin-polarized",
        "nextra-bands",
        "fine-grid-scale",
        "masses",
        "iprint",
        "finite-basis-corr",
        "enthalpy",
        "opt-strategy-bias",
        "force-ewald",
        "num-dump-cycles",
        "velo",
        "md-delta-t",
        "md-ion-t",
        "force-locpot",
        # 'numbers',
        "max-scf-cycles",
        "mass",
        "minim-constant-volume",
        "cut-off-energy",
        "virial",
        "nneightol",
        "max-charge-amp",
        "md-thermostat",
        "md-ensemble",
        "acc",
    ]

    units = {
        "energy": "eV",
        "forces": "eV/Ang",
        "virial": "GPa",
        "oldpos": "Ang",
        "md-temperature": "K",
        "positions": "Ang",
        "avg-ke": "eV",
        "force-nlpot": "eV/Ang",
        "castep-run-time": "s",
        "avgpos": "Ang",
        "md-cell-t": "ps",
        "time": "s",
        "temperature": "K",
        "gap-force": "eV/Ang",
        "gap-energy": "eV",
        "cutoff": "Ang",
        "smearing-width": "eV",
        "pressure": "GPa",
        "gap-virial": "GPa",
        "masses": "_amu",
        "enthalpy": "eV",
        "force-ewald": "eV/Ang",
        "velo": "Ang/s",
        "md-delta-t": "fs",
        "md-ion-t": "ps",
        "force-locpot": "eV/Ang",
        "mass": "g",
        "cut-off-energy": "eV",
        "virial": "GPa",
    }

    dft_settings_map = {
        k: {"field": k, "units": units[k] if k in units else None}
        for k in settings_keys
    }

    dft_settings_map["software"] = "CASTEP"

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "dft-energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": dft_settings_map,
            },
            {
                "energy": {"field": "gap-energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "method": {"value": "GAP"},
                },
            },
        ],
        "atomic-forces": [
            {
                "forces": {"field": "dft-force", "units": "eV/Ang"},
                "_metadata": dft_settings_map,
            },
            {
                "forces": {"field": "gap-force", "units": "eV/Ang"},
                "_metadata": {
                    "method": {"value": "GAP"},
                },
            },
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "dft-virial", "units": "GPa"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": dft_settings_map,
            },
            {
                "stress": {"field": "gap-virial", "units": "GPa"},
                "volume-normalized": {"value": True, "units": None},
                "_metadata": {
                    "method": {"value": "gap"},
                },
            },
        ],
    }

    ids = client.insert_data(
        images,
        property_map=property_map,
        co_md_map={"configuration_type": {"field": "config_type"}},
        transform=tform,
        verbose=True,
    )

    # Used for building groups of configurations for easier analysis/exploration
    configuration_set_regexes = {
        "isolated_atom": "Reference atom",
        "bt": "Beta-tin",
        "dia": "Diamond",
        "sh": "Simple hexagonal",
        "hex_diamond": "Hexagonal diamond",
        "bcc": "Body-centered-cubic",
        "bc8": "BC8",
        "fcc": "Face-centered-cubic",
        "hcp": "Hexagonal-close-packed",
        "st12": "ST12",
        "liq": "Liquid",
        "amorph": "Amorphous",
        "surface_001": "Diamond surface (001)",
        "surface_110": "Diamond surface (110)",
        "surface_111": "Diamond surface (111)",
        "surface_111_pandey": "Pandey reconstruction of diamond (111) surface",
        "surface_111_3x3_das": "Dimer-adatom-stacking-fault (DAS) reconstruction",
        "111adatom": "Configurations with adatom on (111) surface",
        "crack_110_1-10": "Small (110) crack tip",
        "crack_111_1-10": "Small (111) crack tip",
        "decohesion": "Decohesion of diamond-structure Si along various directions",
        "divacancy": "Diamond divacancy configurations",
        "interstitial": "Diamond interstitial configurations",
        "screw_disloc": "Si screw dislocation core",
        "sp": "sp bonded configurations",
        "sp2": "sp2 bonded configurations",
        "vacancy": "Diamond vacancy configurations",
    }

    cs_ids = []
    for i, (regex, desc) in enumerate(configuration_set_regexes.items()):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7)
        )

        cs_id = client.insert_configuration_set(co_ids, description=desc, name=regex)

        cs_ids.append(cs_id)

    all_co_ids, all_pr_ids = list(zip(*ids))  # returned by insert_data()

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


"""
configuration_label_regexes = {
    "isolated_atom": "isolated_atom",
    "bt": "a5",
    "dia": "diamond",
    "sh": "sh",
    "hex_diamond": "sonsdaleite",
    "bcc": "bcc",
    "bc8": "bc8",
    "fcc": "fcc",
    "hcp": "hcp",
    "st12": "st12",
    "liq": "liquid",
    "amorph": "amorphous",
    "surface_001": ["surface", "001"],
    "surface_110": ["surface", "110"],
    "surface_111": ["surface", "111"],
    "surface_111_pandey": ["surface", "111"],
    "surface_111_3x3_das": ["surface", "111", "das"],
    "111adatom": ["surface", "111", "adatom"],
    "crack_110_1-10": ["crack", "110"],
    "crack_111_1-10": ["crac", "111"],
    "decohesion": ["diamond", "decohesion"],
    "divacancy": ["diamond", "vacancy", "divacancy"],
    "interstitial": ["diamond", "interstitial"],
    "screw_disloc": ["screw", "dislocation"],
    "sp": "sp",
    "sp2": "sp2",
    "vacancy": ["diamond", "vacancy"],
}



for regex, labels in configuration_label_regexes.items():
    client.apply_labels(
        dataset_id=ds_id,
        collection_name='configurations',
        query={'names': {'$regex': regex}},
        labels=labels,
        verbose=True
    )
"""


if __name__ == "__main__":
    main(sys.argv[1:])

#!/usr/bin/env python
# coding: utf-8

"""
Possible keys
--------------
"acc",
"avg-ke",
"avgpos",
"calculate-stress",
"castep-file-name",
"castep-run-time",
"continuation",  # input
"cut-off-energy",  # input
"cutoff",  # input
"cutoff-factor",  # input
"damp-mask",
"data-distribution",  # input
"elec-energy-tol",  # input
"enthalpy",
"fine-grid-scale",  # input
"finite-basis-corr",  # input
"fix-occupancy",  # input
"force-ewald",
"force-locpot",
"force-nlpot",
"frac-pos",
"grid-scale",  # input
"hamiltonian",
"i-step",
"in-file",
"iprint",  # input
"kpoints-mp-grid",  # input
"map-shift",
"mass",
"masses",
"max-charge-amp",  # input
"max-scf-cycles",  # input
"md-cell-t",  # input
"md-delta-t",  # input
"md-ensemble",  # input
"md-ion-t",  # inpu
"md-num-iter",  # input
"md-temperature",  # input
"md-thermostat",  # input
"minim-constant-volume",  # input
"minim-hydrostatic-strain",  # input
"minim-lattice-fix",  # input
"mix-history-length",  # input
"mixing-scheme",  # input
"momenta",
"n-neighb",
"nextra-bands",  # input
"nneightol",  # input
"num-dump-cycles",  # input
"oldpos",
"opt-strategy",  # input
"opt-strategy-bias",  # input
"popn-calculate",  # input
"pressure",
"reuse",  # input
"smearing-width",  # input
"spin-polarized",  # input
"task",  # input
"temperature",  # input
"thermostat-region",  # input
"time",
"travel",
"velo",
"virial",
"xc-functional"
"""
from argparse import ArgumentParser
from pathlib import Path
import sys


from ase.io import iread
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    potential_energy_pd,
)
import numpy as np

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/data/gap_si/")
DATASET_FP = Path().cwd().parent / "data/Si_PRX_GAP"

DATASET = "Si_PRX_GAP"

PUBLICATION = "https://doi.org/10.1103/PhysRevX.8.041048"
DATA_LINK = "https://doi.org/10.17863/CAM.65004"
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

PI_MD_DFT = {
    "software": {"value": "CASTEP"},
    "method": {"field": "method"},
    "input": {"field": "input"},
}
PI_MD_GAP = {"method": {"value": "GAP"}}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "dft-energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD_DFT,
        },
        {
            "energy": {"field": "gap-energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD_GAP,
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "dft-force", "units": "eV/Ang"},
            "_metadata": PI_MD_DFT,
        },
        {
            "forces": {"field": "gap-force", "units": "eV/Ang"},
            "_metadata": PI_MD_GAP,
        },
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "dft-virial", "units": "GPa"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD_DFT,
        },
        {
            "stress": {"field": "gap-virial", "units": "GPa"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD_GAP,
        },
    ],
}

INPUT = [
    "calculate-stress",
    "continuation",
    "cut-off-energy",
    "cutoff",
    "cutoff-factor",
    "data-distribution",
    "elec-energy-tol",
    "fine-grid-scale",
    "finite-basis-corr",
    "fix-occupancy",
    "grid-scale",
    "iprint",
    "kpoints-mp-grid",
    "max-charge-amp",
    "max-scf-cycles",
    "md-cell-t",
    "md-delta-t",
    "md-ensemble",
    "md-ion-t",
    "md-num-iter",
    "md-temperature",
    "md-thermostat",
    "minim-constant-volume",
    "minim-hydrostatic-strain",
    "minim-lattice-fix",
    "mix-history-length",
    "mixing-scheme",
    "nextra-bands",
    "nneightol",
    "num-dump-cycles",
    "opt-strategy",
    "opt-strategy-bias",
    "popn-calculate",
    "reuse",
    "smearing-width",
    "spin-polarized",
    "task",
    "temperature",
    "thermostat-region",
    "xc-functional",
]
CO_MD = {
    key: {"field": key}
    for key in [
        "acc",
        "avg-ke",
        "avgpos",
        "castep-file-name",
        "castep-run-time",
        "damp-mask",
        "enthalpy",
        "force-ewald",
        "force-locpot",
        "force-nlpot",
        "frac-pos",
        "hamiltonian",
        "i-step",
        "in-file",
        "map-shift",
        "mass",
        "masses",
        "momenta",
        "n-neighb",
        "oldpos",
        "pressure",
        "time",
        "travel",
        "velo",
        "virial",
    ]
}


def reader(fp):
    for atom in iread(fp, index=":"):
        atom.info = {
            key.replace("_", "-").lower(): val for key, val in atom.info.items()
        }
        input = dict()
        for key, val in atom.info.items():
            if isinstance(val, np.int64):
                val = int(val)
            if key == "dft-virial" or key == "gap-virial":
                atom.info[key] = val.reshape((3, 3)).tolist()
            if isinstance(val, np.ndarray):
                val = val.tolist()
            elif key in [
                "md-temperature",
                "md-cell-t",
                "smearing-width",
                "md-delta-t",
                "md-ion-t",
                "cut-off-energy",
                "elec-energy-tol",
            ]:
                if isinstance(val, str) and all(
                    [x.isdigit() or x in ["-", "."] for x in val.split()[0]]
                ):
                    atom.info[key] = float(val.split()[0])

            if key in INPUT:
                if key in units:
                    input[key] = {key: val, "unit": units.get(key)}
                else:
                    input[key] = {key: val}
        if len(input) > 0:
            atom.info["input"] = input
        atom.info["per-atom"] = False
        atom.info["method"] = f"DFT-{atom.info.get('xc-functional', 'PW91')}"
        name = atom.info.get("config-type")
        if name is not None:
            atom.info["name"] = name
        else:
            atom.info["name"] = "si_prx_gap"
        yield atom


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

    images = list(
        load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=["Si"],
            reader=reader,
            glob_string="gp_iter6_sparse9k.xml.xyz",
            verbose=True,
        )
    )
    ds_id = generate_ds_id()
    ids = client.insert_data(
        images,
        ds_id=ds_id,
        property_map=property_map,
        co_md_map=CO_MD,
        verbose=True,
    )

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

        cs_id = client.insert_configuration_set(
            co_ids, ds_id=ds_id, description=desc, name=f"si_prx_gap_{regex}"
        )

        cs_ids.append(cs_id)

    all_co_ids, all_pr_ids = list(zip(*ids))  # returned by insert_data()

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
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

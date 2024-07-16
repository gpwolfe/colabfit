"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
fund = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
optb88vdw_total_energy (dft_3d)
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

Polymer genome keys:
Original keys (from publication):

Source: VSharma_etal:NatCommun.5.4845(2014)
Class: organic_polymer_crystal
Label: Polyimide
Structure prediction method used: USPEX
Number of atoms: 32
Number of atom types: 4
Atom types: C H O N
Dielectric constant, electronic: 3.71475E+00
Dielectric constant, ionic: 1.54812E+00
Dielectric constant, total: 5.26287E+00
Band gap at the GGA level (eV): 2.05350E+00
Band gap at the HSE06 level (eV): 3.30140E+00
Atomization energy (eV/atom): -6.46371E+00
Volume of the unit cell (A^3): 2.79303E+02

Keys
['Cv',      <-- heat capacity
 'G',       <-- Free energy at 298.15K
 'H',       <-- enthalpy
 'HOMO',
 'LUMO',
 'R2',      <--elec. spatial extent
 'SMILES',
 'SMILES_relaxed',
 'U',       <-- internal energy at 298.15K
 'U0',      <-- internal energy at 0K
 'ZPVE',
 'alpha',
 'atoms',
 'gap',     <-- gap
 'id',
 'mu',
 'omega1']
"""


import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client

from colabfit.tools.property_definitions import free_energy_pd, potential_energy_pd


DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "qm9_std_jctc.json"
DS_NAME = "JARVIS_QM9_STD_JCTC"
DS_DESC = (
    "The JARVIS_QM9_STD_JCTC dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the QM9 dataset, originally created as part of the datasets "
    "at quantum-machine.org. Units for r2 (electronic spatial extent) are a\0^2; for "
    "alpha (isotropic polarizability), a\0^3; for mu (dipole moment), D; for Cv (heat "
    "capacity), cal/mol K. Units for all other properties are eV. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
    "For the first iteration of DFT calculations, Gaussian 09’s default electronic "
    "and geometry thresholds have been used for all molecules. For those molecules "
    "which failed to reach SCF convergence ultrafine grids have been invoked within "
    "a second iteration for evaluating the XC energy contributions. Within a third "
    "iteration on the remaining unconverged molecules, we identified those which "
    "had relaxed to saddle points, and further tightened the SCF criteria using "
    "the keyword scf(maxcycle=200, verytight). All those molecules which still "
    "featured imaginary frequencies entered the fourth iteration using keywords, "
    "opt(calcfc, maxstep=5, maxcycles=1000). calcfc constructs a Hessian in the "
    "first step of the geometry relaxation for eigenvector following. Within the "
    "fifth and final iteration, all molecules which still failed to reach "
    "convergence, have subsequently been converged using opt(calcall, maxstep=1, "
    "maxcycles=1000)"
)

LICENSE = "https://creativecommons.org/licenses/by/4.0/"

PUBLICATION = "https://doi.org/10.1038/sdata.2014.22"
DATA_LINK = "https://ndownloader.figshare.com/files/28715319"
OTHER_LINKS = ["https://jarvis.nist.gov/", "http://quantum-machine.org/datasets/"]
LINKS = [
    "https://doi.org/10.1038/sdata.2014.22",
    "https://jarvis.nist.gov/",
    "http://quantum-machine.org/datasets/",
    "https://ndownloader.figshare.com/files/28715319",
]
AUTHORS = [
    "Raghunathan Ramakrishnan",
    "Pavlo O. Dral",
    "Matthias Rupp",
    "O. Anatole von Lilienfeld",
]
ELEMENTS = None

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "U0", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "Gaussian 09"},
                "method": {"value": "DFT-B3LYP"},
                "basis-set": {"value": "6-31G(2df,p)"},
                "temperature": {"value": "0K"},
            },
        },
        {
            "energy": {"field": "U", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "Gaussian 09"},
                "method": {"value": "DFT-B3LYP"},
                "basis-set": {"value": "6-31G(2df,p)"},
                "temperature": {"value": "298.15K"},
            },
        },
    ],
    "free-energy": [
        {
            "energy": {"field": "G", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": {
                "software": {"value": "Gaussian 09"},
                "method": {"value": "DFT-B3LYP"},
                "basis-set": {"value": "6-31G(2df,p)"},
                "temperature": {"value": "0K"},
            },
        },
    ],
    "band-gap": [
        {
            "energy": {"field": "gap", "units": "eV"},
            "_metadata": {
                "software": {"value": "Gaussian 09"},
                "method": {"value": "DFT-B3LYP"},
                "basis-set": {"value": "6-31G(2df,p)"},
            },
        },
    ],
}


# with open("atomization_energy.json", "r") as f:
#     atomization_energy_pd = json.load(f)
with open("band_gap.json", "r") as f:
    band_gap_pd = json.load(f)


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
    configs = []
    for i, row in enumerate(data):
        atoms = row.pop("atoms")
        if atoms["cartesian"] is True:
            config = AtomicConfiguration(
                positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )
        else:
            config = AtomicConfiguration(
                scaled_positions=atoms["coords"],
                symbols=atoms["elements"],
                cell=atoms["lattice_mat"],
            )

        config.info["name"] = f"{fp.stem}_{i}"

        for key, val in row.items():
            if isinstance(val, str) and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif isinstance(val, dict) and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (isinstance(val, float) or isinstance(val, int)) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
            else:
                pass
        configs.append(config)
    return configs


def main(argv):
    client = get_client(argv)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB,
        generator=False,
    )

    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(band_gap_pd)
    client.insert_property_definition(potential_energy_pd)

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

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        data_license=LICENSE,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


CO_KEYS = [
    "Cv",  # <-- heat capacity
    # "G",  # <-- Free energy at 298.15K
    "H",  # <-- enthalpy
    "HOMO",
    "LUMO",
    "R2",  # <--elec. spatial extent
    "SMILES",
    "SMILES_relaxed",
    # "U",  # <-- internal energy at 298.15K
    # "U0",  # <-- internal energy at 0K
    "ZPVE",
    "alpha",
    # "atoms",
    # "gap",  # <-- gap
    "id",
    "mu",
    "omega1",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

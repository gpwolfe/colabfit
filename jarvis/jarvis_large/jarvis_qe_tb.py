"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
func = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

QE-TB property keys

['atoms',
 'crystal_system',
 'energy_per_atom', <- potential energy, per-atom=True eV
 'f_enp',           <- formation energy eV
 'final_energy',    <- potential energy, per-atom=False eV
 'forces',          <- atomic forces eV/A
 'formula',
 'indir_gap',       <- band gap eV
 'jid',
 'natoms',
 'source_folder',
 'spacegroup_number',
 'spacegroup_symbol',
 'stress']          <- cauchy stress eV/A
"""


import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data
from colabfit_utilities import get_client
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    cauchy_stress_pd,
    atomic_forces_pd,
)

DATASET_FP = Path().cwd().parent / "jarvis_json/"
GLOB = "jqe_tb_folder.json"
DS_NAME = "JARVIS-QE-TB"
DS_DESC = (
    "The QE-TB dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) DFT database. This subset contains "
    "configurations generated in Quantum ESPRESSO. "
    "JARVIS is a set of tools and datasets built to meet current materials design "
    "challenges."
)

PUBLICATION = "https://doi.org/10.1103/PhysRevMaterials.7.044603"
DATA_LINK = "https://ndownloader.figshare.com/files/29070555"
OTHER_LINKS = ["https://jarvis.nist.gov/", "https://arxiv.org/abs/2112.11585"]
LINKS = [
    "https://arxiv.org/abs/2112.11585",
    "https://doi.org/10.1103/PhysRevMaterials.7.044603",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/29070555",
]
AUTHORS = ["Kevin F. Garrity", "Kamal Choudhary"]
ELEMENTS = None
PI_MD = {
    "software": {"value": "Quantum ESPRESSO"},
    "method": {"value": "DFT-PBEsol"},
    "input": {
        "value": {
            "encut": {"value": "45 Ry"},
            "pseudopotentials": "GBRV",
            "k-point-grid-density": {"value": 29, "units": "Ang^-1"},
            "smearing": "Gaussian",
            "smearing-energy": {"value": 0.01, "units": "Ry"},
        }
    },
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "final_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        },
        {
            "energy": {"field": "energy_per_atom", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_MD,
        },
    ],
    "formation-energy": [
        {
            "energy": {"field": "f_enp", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "stress", "units": "eV/angstrom"},
            "volume-normalized": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
    "band-gap": [
        {
            "energy": {"field": "indir_gap", "units": "eV"},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_MD,
        }
    ],
}


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
                config.info[key] = val
            elif isinstance(val, list) and len(val) > 0 and any([x != "" for x in val]):
                config.info[key] = val
            elif isinstance(val, dict) and all([v != "na" for v in val.values()]):
                config.info[key] = val
            elif isinstance(val, float) or isinstance(val, int):
                config.info[key] = val
            else:
                pass
        configs.append(config)
    return configs


with open("formation_energy.json", "r") as f:
    formation_energy_pd = json.load(f)
with open("band_gap.json", "r") as f:
    band_gap_pd = json.load(f)


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

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(formation_energy_pd)
    client.insert_property_definition(band_gap_pd)

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
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK] + OTHER_LINKS,
        description=DS_DESC,
        verbose=False,
    )


CO_KEYS = [
    # "atoms",            <- atom structures
    "crystal_system",
    #  'energy_per_atom', <- potential energy, per-atom=True eV
    #  'f_enp',           <- formation energy eV
    #  'final_energy',    <- potential energy, per-atom=False eV
    #  'forces',          <- atomic forces eV/A
    "formula",
    #  'indir_gap',       <- band gap eV
    "jid",
    "natoms",
    "source_folder",
    "spacegroup_number",
    "spacegroup_symbol",
    #  'stress'           <- cauchy stress eV/A
]

CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

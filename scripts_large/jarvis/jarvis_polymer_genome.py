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

Keys from JARVIS-polymer-genome
[
'atom_en',  <-- atomization energy/atom
 'atoms',
 'diel_elec',
 'diel_ion',
 'diel_tot',
 'gga_gap',  <-- gap calculated with rPW86 functional
 'hse_gap',  <-- gap calc'd with HSE06 functional
 'id',
 'label',
 'method',
 'src',
 'vol'
 ]

"""

from argparse import ArgumentParser
import json
from numpy import isnan
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase

# from colabfit.tools.property_definitions import potential_energy_pd

DATASET_FP = Path("jarvis_json_zips/")
GLOB = "pgnome.json"
DS_NAME = "JARVIS-Polymer-Genome"
DS_DESC = (
    "The JARVIS-Polymer-Genome dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "configurations from the Polymer Genome dataset, as created for the linked "
    "publication (Huan, T., Mannodi-Kanakkithodi, A., Kim, C. et al.). Structures "
    "were curated from existing sources and the original authors' works, removing "
    "redundant, identical structures before calculations, and removing redundant "
    "datapoints after calculations were performed. Band gap energies were calculated "
    "using two different DFT functionals: rPW86 and HSE06; atomization energy was "
    "calculated using rPW86. "
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

LINKS = [
    "https://doi.org/10.1088/2053-1583/aacfc1",
    "https://jarvis.nist.gov/",
    "https://ndownloader.figshare.com/files/28682010",
]
AUTHORS = [
    "Tran Doan Huan",
    "Arun Mannodi-Kanakkithodi",
    "Chiho Kim",
    "Vinit Sharma",
    "Ghanshyam Pilania",
    "Rampi Ramprasad",
]
ELEMENTS = None
PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-rPW86"},
    "ecut": {"value": "400 eV"},
}
BG_HSE_MD = {"software": {"value": "VASP"}, "method": {"value": "DFT-HSE06"}}

PROPERTY_MAP = {
    "atomization-energy": [
        {
            "energy": {"field": "atom_en", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_MD,
        },
    ],
    "band-gap": [
        {
            "energy": {"field": "gga_gap", "units": "eV"},
            "_metadata": PI_MD,
        },
        {
            "energy": {"field": "hse_gap", "units": "eV"},
            "_metadata": BG_HSE_MD,
        },
    ],
}


with open("atomization_energy.json", "r") as f:
    atomization_energy_pd = json.load(f)
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
            if type(val) == str and val != "na" and len(val) > 0:
                config.info[key.replace(" ", "-")] = val
            elif type(val) == list and len(val) > 0 and any([x != "" for x in val]):
                config.info[key.replace(" ", "-")] = val
            elif type(val) == dict and not all([v != "na" for v in val.values()]):
                config.info[key.replace(" ", "-")] = val
            elif (type(val) == float or type(val) == int) and not isnan(val):
                config.info[key.replace(" ", "-")] = val
            else:
                pass
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
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB,
        generator=False,
    )

    # client.insert_property_definition(formation_energy_pd)
    client.insert_property_definition(band_gap_pd)
    client.insert_property_definition(atomization_energy_pd)

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


CO_KEYS = [
    # "atom_en",
    # "atoms",
    "diel_elec",
    "diel_ion",
    "diel_tot",
    # "gga_gap",
    # "hse_gap",
    "id",
    "label",
    "method",
    "src",
    "vol",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


if __name__ == "__main__":
    main(sys.argv[1:])

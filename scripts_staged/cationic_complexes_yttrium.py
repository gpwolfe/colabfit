"""
author: Gregory Wolfe

Properties
----------
energy

Other properties added to metadata
----------------------------------
enthalpy
gibbs energy
dispersion

File notes
----------

xyz file header has been reformatted to be readable with ase.io.read using below:

header = re.compile(
    r"(?P<name>[\S\_]+)\sE:\s(?P<energy>\-?\d+\.\d+)\  # noqa: W605
        \s;\sH:\s(?P<enthalpy>\-?\d+\.\d+)\
        \s;\sG:\s(?P<gibbs>\-?\d+\.\d+)\s;\sD:\s\
        (?P<dispersion>\-?\d+\.\d+).*"
)
with open("om2c00238_si_001.xyz", "r") as f:
    newtext = []
    for line in f.readlines():
        lmatch = header.match(line)
        if lmatch:
            newtext.append(
                f"name={lmatch['name']} energy={lmatch['energy']} \
                    enthalpy={lmatch['enthalpy']} gibbs={lmatch['gibbs']} \
                        dispersion={lmatch['dispersion']}\n"
            )
        else:
            newtext.append(line)

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    # atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/cationic_complexes_yttrium")
DATASET_NAME = "Cationic_phenoxyimine_complexes_of_yttrium"

SOFTWARE = "Gaussian 09"
METHODS = "DFT-B3PW91"

PUBLICATION = "https://doi.org/10.1021/acs.organomet.2c00238"
DATA_LINK = "https://doi.org/10.1021/acs.organomet.2c00238.s001"
LINKS = [
    "https://doi.org/10.1021/acs.organomet.2c00238.s001",
    "https://doi.org/10.1021/acs.organomet.2c00238",
]
AUTHORS = [
    "Alexis D. Oswald",
    "Ludmilla Verrieux",
    "Pierre-Alain R. Breuil",
    "Hélène Olivier-Bourbigou",
    "Julien Thuilliez",
    "Florent Vaultier",
    "Mostafa Taoufik",
    "Lionel Perrin",
    "Christophe Boisson",
]
DATASET_DESC = (
    "This dataset contains DFT calculations that were carried out in conjunction with "
    "experimental investigation of a cationic phenoxyimine yttrium complex as an "
    "isoprene polimerization catalyst. Calculations were performed using the "
    "Gaussian 09 D.01 suite of programs.Electronic structure calculations "
    "were performed at the DFT level using the B3PW91 functional. The "
    "Stuttgart-Cologne small-core quasi-relativistic pseudopotential ECP28MWB "
    "and its available basis set including up to the g function were used to describe "
    "yttrium. Similarly, silicon and phosphorus were represented by a "
    "Stuttgart-Dresden-Bonn pseudopotential along with the related basis set "
    "augmented by a d function of polarization (αd(P) = 0.387 and αd(Si) = 0.284). "
    "Other atoms were described by a polarized all-electron triple-ζ 6-311G(d,p) basis "
    "set. Bulk solvent effect of toluene or THF was simulated using the SMD "
    "continuum model. The Grimme empirical correction with the original D3 damping "
    "function was used to include the dispersion correction as a single-point "
    "calculation. Transition-state optimization was followed by frequency "
    "calculations to characterize the stationary point. Intrinsic reaction coordinate "
    "calculations were performed to confirm the connectivity of the transition "
    "states. Gibbs energies were estimated within the harmonic oscillator "
    "approximation and estimated at 298 K and 1 atm."
)
ELEMENTS = None
GLOB_STR = "*reformatted.xyz"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    # "basis-set": {"field": "basis_set"}
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "enthalpy": {"field": "enthalpy"},
    "dispersion": {"field": "dispersion"},
    "gibbs-energy": {"field": "gibbs"},
}

CSS = None


def reader(filepath: Path):
    configs = read(filepath, index=":")
    for i, config in enumerate(configs):
        name = config.info["name"]
        config.info["name"] = f"cationic_phenoxyimine_complexes_yttrium__{name}"

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
    # client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )

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
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DATASET_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

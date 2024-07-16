"""
author: Gregory Wolfe

Properties
----------
energy

Other properties added to metadata
----------------------------------
None

File notes
----------

xyz file header has been reformatted with the following
with open("data/yttrium_catalyzed_alkylations/om8b00397_si_002.xyz", "r") as f:
    new_text = []
    for line in f.readlines():
        if "," in line:
            header = [e.strip() for e in line.strip().split(",")]
            name = header[0]
            if all([x.isdigit() for x in name]):
                name += "_"
            new_header = f"name={name}"
            for piece in header[1:]:
                if "Imaginary" not in piece and not en_re.match(piece):
                    new_header += f"_{piece}"
                elif "Imaginary" in piece:
                    new_header += f" imaginary_frequency={piece.split(' = ')[1]}"
                elif en_re.match(piece):
                    new_header += f" energy={piece}"
                else:
                    print(piece)

            new_text.append(new_header + "\n")
        else:
            new_text.append(line)

"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/yttrium_catalyzed_alkylations")
DATASET_NAME = (
    "Yttrium-catalyzed_benzylic_C-H_alkylations_of_alkylpyridines_with_olefins"
)

SOFTWARE = "Gaussian 09"
METHODS = "DFT-M06-L"

PUBLICATION = "https://doi.org/10.1021/acs.organomet.8b00397"
DATA_LINK = "https://doi.org/10.1021/acs.organomet.8b00397.s002"
LINKS = [
    "https://doi.org/10.1021/acs.organomet.8b00397.s002",
    "https://doi.org/10.1021/acs.organomet.8b00397",
]
AUTHORS = ["Guangli Zhou", "Gen Luo", "Xiaohui Kang", "Zhaomin Hou", "Yi Luo"]
DATASET_DESC = (
    "This data was assembled to investigate rare-earth-catalyzed benzylic C(sp3)-H "
    "addition of pyridines to olefins. "
    "All calculations were performed with the Gaussian 09 software package. "
    "The B3PW91 functional was used for geometric optimization without "
    "any symmetric constraints. Each optimized structure was subsequently analyzed "
    "by harmonic vibrational frequencies at the same level of theory for "
    "characterization of a minimum (NImag = 0) or a transition state (NImag = 1) "
    "to obtain the thermodynamic data. The 6-31G(d) basis set was used for C, H, "
    "and N atoms, and Stuttgart/Dresden relativistic effective core potentials "
    "(RECPs) as well as the associated valence basis sets were used for the Y "
    "atom. To obtain more accurate energies, single-point energy calculations "
    "were performed with a larger basis set. In such single-point calculations, "
    "the M06-L functional, which often shows good performance in the treatment "
    "of transition-metal systems, was used together with the CPCM solvation "
    "model for consideration of the toluene solvation effect. The same basis "
    "set together with associated pseudopotentials as in geometry optimization "
    "was used for the Y atom, and the 6-311+G(d,p) basis set was used for the "
    "remaining atoms."
)
ELEMENTS = None
GLOB_STR = "yttrium_catalyzed_reformatted.xyz"

PI_METADATA = {
    "software": {"value": SOFTWARE},
    "method": {"value": METHODS},
    "basis-set": {"field": "basis_set"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "hartree"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "imaginary-frequency": {"field": "imaginary_frequency"},
}


def reader(filepath: Path):
    configs = read(filepath, index=":")
    for i, config in enumerate(configs):
        name = f"yttrium_catalyzed_alkylations_{config.info['name']}"
        config.info["name"] = name
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
    client.insert_property_definition(atomic_forces_pd)
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
        links=[PUBLICATION, DATA_LINK],
        description=DATASET_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

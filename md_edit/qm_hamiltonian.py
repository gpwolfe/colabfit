"""
author:gpwolfe

Data at:
http://quantum-machine.org/datasets/
File addresses:
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_water.tgz
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_ethanol_dft.tgz
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_malondialdehyde.tgz
http://quantum-machine.org/data/schnorb_hamiltonian/schnorb_hamiltonian_uracil.tgz

Extract files:
for f in schnorb*.tgz; do tar zxf "$f" -C $project_dir/scripts/qm_hamiltonian; done

Change DB_PATH to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
None

File notes
----------
The schnorb_hamiltonian_ethanol_hf file is not included, as it is run at a
different level of theory, and there is already an ethanol file at DFT level.

"""
from argparse import ArgumentParser
from ase.db import connect
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
from pathlib import Path
import sys

DB_PATH = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/qm_hamiltonian")

DS_NAME = "QM_hamiltonian_nature_2019"
AUTHORS = [
    "Kristof T. Schütt",
    "Michael Gastegger",
    "Alexandre Tkatchenko",
    "Klaus-Robert Müller",
    "Reinhard J. Maurer",
]
PUBLICATION = "https://doi.org/10.1038/s41467-019-12875-2"
DATA_LINK = "http://quantum-machine.org/datasets/"
LINKS = [
    "http://quantum-machine.org/datasets/",
    "https://doi.org/10.1038/s41467-019-12875-2",
]
DS_DESC = (
    "~100,000 configurations of water, ethanol, "
    "malondialdehyde and uracil gathered at the PBE/def2-SVP "
    "level of theory using ORCA."
)


def reader(filepath):
    filepath = Path(filepath)
    db = connect(filepath)
    atoms = []
    for row in db.select():
        atom = row.toatoms()
        atom.info = row.data
        atom.info["name"] = filepath.stem.split("_")[-1]
        atoms.append(atom)
        if type(atom.info["energy"] == list):
            atom.info["energy"] = float(atom.info["energy"][0])
    return atoms


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

    configurations = load_data(
        file_path=DB_PATH,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="*.db",
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    metadata = {
        "software": {"value": "ORCA"},
        "method": {"value": "DFT-PBE"},  # SchNOrb
        "basis-set": {"value": "def2-SVP"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            "QM_Hamiltonian_water",
            "water",
            "All water configurations from the Quantum Machine Molecular \
            Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_ethanol_dft",
            "dft",
            "All ethanol dft configurations from the Quantum Machine \
                Molecular Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_malondialdehyde",
            "malondialdehyde",
            "All malondialdehyde configurations from the Quantum Machine \
                Molecular Hamiltonians and Overlap Matrices set",
        ],
        [
            "QM_Hamiltonian_uracil",
            "uracil",
            "All uracil configurations from the Quantum Machine Molecular \
                Hamiltonians and Overlap Matrices set",
        ],
    ]
    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_ids = []
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

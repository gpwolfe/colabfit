"""
Dimer charge, Dimer multiplicity, Momomer A charge, Monomer A multiplicity, Momomer B charge, Monomer B multiplicity, # of atoms in Monomer A, # of atoms in Monomer B, CCSD(T)/CBS,   CCSD(T)/haTZ,   MP2/haTZ,   MP2/CBS,   MP2/aTZ,   MP2/aQZ,   HF/haTZ,   HF/aTZ,   HF/aQZ,   SAPT2+/aDZ Tot,   SAPT2+/aDZ Elst,   SAPT2+/aDZ Exch,   SAPT2+/aDZ Ind,   SAPT2+/aDZ Disp
"""

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd

from argparse import ArgumentParser
from ase.io.extxyz import read_xyz
from pathlib import Path
import sys


DATASET_FP = Path("/large_data/new_raw_datasets_2.0/nenci2021/nenci2021/xyzfiles/")
DATASET_FP = Path("data/nenci2021/xyzfiles")  # remove

DS_NAME = "NENCI-2021"
AUTHORS = [
    "Zachary M. Sparrow",
    "Brian G. Ernst",
    "Paul T. Joo",
    "Ka Un Lao",
    "Robert A. DiStasio, Jr",
]
LINKS = [
    "https://doi.org/10.1063/5.0068862",
]

DS_DESC = (
    "NENCI-2021 is a database of approximately 8000 benchmark Non-Equilibirum "
    "Non-Covalent Interaction (NENCI) energies performed on molecular dimers;"
    "intermolecular complexes of biological and chemical relevance with a "
    "particular emphasis on close intermolecular contacts. Based on dimers"
    "from the S101 database."
)
PROPS = [
    "dimer_charge",
    "dimer_multiplicity",
    "momomer_a_charge",
    "monomer_a_multiplicity",
    "momomer_b_charge",
    "monomer_b_multiplicity",
    "natoms_monomer_a",
    "natoms_monomer_b",
    "CCSD(T)/CBS",
    "CCSD(T)/haTZ",  # heavy-aug-cc-pVTZ
    "MP2/haTZ",  # heavy-aug-cc-pVTZ
    "MP2/CBS",
    "MP2/aTZ",  #  aug-cc-pVTZ
    "MP2/aQZ",  #  aug-cc-pVQZ
    "HF/haTZ",  # heavy-aug-cc-pVTZ
    "HF/aTZ",  #  aug-cc-pVTZ
    "HF/aQZ",  #  aug-cc-pVQZ
    "sapt_total",  #  aug-cc-pVDZ
    "sapt_electrostatics",
    "sapt_exchange",
    "sapt_induction",
    "sapt_dispersion",
]


def nenci_props_parser(string):
    s = [float(x) for x in string.split()]
    return dict(zip(PROPS, s))


def reader(file_path):
    print(file_path.name)
    with open(file_path, "r") as f:
        atom = next(read_xyz(f, properties_parser=nenci_props_parser))
    atom.info["name"] = file_path.stem
    yield atom


co_md = {
    "dimer-charge": {"field": "dimer_charge"},
    "dimer-multiplicity": {"field": "dimer_multiplicity"},
    "momomer_a_charge": {"field": "momomer_a_charge"},
    "monomer_a_multiplicity": {"field": "monomer_a_multiplicity"},
    "momomer_b_charge": {"field": "momomer_b_charge"},
    "monomer_b_multiplicity": {"field": "monomer_b_multiplicity"},
    "num_atoms_monomer_a": {"field": "natoms_monomer_a"},
    "num_atoms_monomer_b": {"field": "natoms_monomer_b"},
    "SAPT2+/aDZ-electrostatics": {"field": "sapt_electrostatics"},
    "SAPT2+/aDZ-exchange": {"field": "sapt_exchange"},
    "SAPT2+/aDZ-induction": {"field": "sapt_induction"},
    "SAPT2+/aDZ-dispersion": {"field": "sapt_dispersion"},
}
CS_COMBOS = set()
for f in DATASET_FP.glob("*.xyz"):
    CS_COMBOS.add(tuple(f.stem.split("_")[1].split("-")))


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
    client.insert_property_definition(potential_energy_pd)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=None,
        reader=reader,
        glob_string="*.xyz",
        verbose=True,
        generator=False,
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "CCSD(T)/CBS", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": {
                    "software": {"value": "Psi4"},
                    "method": {"value": "CCSD(T)"},
                    "basis-set": {"value": "CBS"},
                },
            }
        ],
        "potential-energy": [
            {
                "energy": {"field": "sapt_total", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": {
                    "software": {"value": "Psi4"},
                    "method": {"value": "SAPT2+"},
                    "basis-set": {"value": "aug-cc-pVDZ"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations,
            co_md_map=co_md,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )
    print(ids)

    all_co_ids, all_pr_ids = list(zip(*ids))
    cs_info = []
    for mon_a, mon_b in CS_COMBOS:
        # name, regex, desciption
        cs_info.append(
            (
                f"{mon_a.lower()}-{mon_b.lower()}",
                f"Dimers containing {mon_a.lower()} as monomer A and {mon_b.lower()} as monomer B",
                f"{mon_a}-{mon_b}",
            )
        )

    cs_ids = []

    for name, desc, reg in cs_info:
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            query={"names": {"$regex": reg}},
            name=name,
            description=desc,
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_pr_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

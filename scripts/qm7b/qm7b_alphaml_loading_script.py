"""
author:gpwolfe

Download from:
https://doi.org/10.24435/materialscloud:2019.0002/v3

Folders should be SCAN0_DADZ, B3LYP_daTZ, BELYP_daDZ, and CCSD_daDZ

Unzip files to single parent directory before running script:
mkdir data/qm7_alphaml
for f in *da*Z.tar.gz; do tar zxf "$f" -C <project_dir>/scripts/qm7b; done

Change DB_PATH to reflect location of parent folder if necessary
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy

Other properties added to metadata:
HOMO and LUMO energy

File notes
----------
A number of other properties could be added to the metadata from these
files, including dipole and quadrupole moments, same and opposite spin,
and polarizability
"""
from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import potential_energy_pd
from collections import defaultdict
from pathlib import Path
import re
import sys


DATASET_PATH = Path().cwd()

# Regex parsers for header line and coordinate lines

# For CCSD folder
# Pattern to match (in some files, some num. values replaced by hyphen)
# Properties,75.615177454043703,4.939919779917851,78.387507863214978,75.767797915524383,72.690226583391762,0.001369360429452,0.044615618191013,-0.007312056508349,0.0021,-0.5290,-0.1546,1.5527,-2.8038,1.2511,0.0195,0.0045,-1.5023,-288.067480970457609,-0.260956266998665,-0.795712498569882,-0.238171179770555,-0.885417704524427

CCSD_HEADER_RE = re.compile(
    r"Properties,(?P<iso_di_pol>[-\d\.]+),(?P<aniso_di_pol>[-\d\.]+),"
    r"(?P<di_pol_1>[-\d\.]+),(?P<di_pol_2>[-\d\.]+),"
    r"(?P<di_pol_3>[-\d\.]+),(?P<di_pol_4>[-\d\.]+),"
    r"(?P<di_pol_5>[-\d\.]+),(?P<di_pol_6>[-\d\.]+),"
    r"(?P<di_moment_1>[-\d\.]+),(?P<di_moment_2>[-\d\.]+),"
    r"(?P<di_moment_3>[-\d\.]+),(?P<quad_moment_1>[-\d\.]+),"
    r"(?P<quad_moment_2>[-\d\.]+),(?P<quad_moment_3>[-\d\.]+),"
    r"(?P<quad_moment_4>[-\d\.]+),(?P<quad_moment_5>[-\d\.]+),"
    r"(?P<quad_moment_6>[-\d\.]+),(?P<total_energy>[-\d\.]+),"
    r"(?P<same_spin_mp2_corr>[-\d\.]+),(?P<oppos_spin_mp2_corr>[-\d\.]+),"
    r"(?P<same_spin_ccsd_corr>[-\d\.]+),"
    r"(?P<oppos_spin_ccsd_corr>[-\d\.]+)$"
)

# B3LYP and SCAN0 folders
# Pattern to match (in some files, some num. values replaced by hyphen)
# Properties,16.939854052467204,0.003828440619037,16.942383975424679,16.938517010826502,16.938661171150429,-0.000267048928424,0.000028373634520,0.000089186068862,-0.0000,-0.0001,0.0000,-0.0001,0.0000,0.0001,0.0001,-0.0001,-0.0001,-40.48621978,-0.411,0.006

B3LYP_SCAN0_HEADER_RE = re.compile(
    r"Properties,(?P<iso_di_pol>[-\d\.]+),(?P<aniso_di_pol>[-\d\.]+),"
    r"(?P<di_pol_1>[-\d\.]+),(?P<di_pol_2>[-\d\.]+),(?P<di_pol_3>[-\d\.]+),"
    r"(?P<di_pol_4>[-\d\.]+),(?P<di_pol_5>[-\d\.]+),(?P<di_pol_6>[-\d\.]+),"
    r"(?P<di_moment_1>[-\d\.]+),(?P<di_moment_2>[-\d\.]+),"
    r"(?P<di_moment_3>[-\d\.]+),(?P<quad_moment_1>[-\d\.]+),"
    r"(?P<quad_moment_2>[-\d\.]+),(?P<quad_moment_3>[-\d\.]+),"
    r"(?P<quad_moment_4>[-\d\.]+),(?P<quad_moment_5>[-\d\.]+),"
    r"(?P<quad_moment_6>[-\d\.]+),(?P<total_energy>[-\d\.]+),"
    r"(?P<homo_energy>[-\d\.]+),(?P<lumo_energy>[-\d\.]+)$"
)

# For element/coordinate lines in all folders
# Pattern to match:
# O	-0.9033863347	-2.7689731175	-0.4116379574
COORD_RE = re.compile(
    r"(?P<element>[a-zA-Z]{1,2})\s+(?P<x>[-\d\.]+)\s+"
    r"(?P<y>[-\d\.]+)\s+(?P<z>[-\d\.]+)$"
)
# File parsing and reading functions


def properties_parser(re_match, line):
    groups = re_match.match(line)
    return groups.groupdict().items()


def xyz_parser(file_path, header_regex):
    file_path = Path(file_path)
    name = f"qm7b_{file_path.parent.name}"
    elem_coords = defaultdict(list)
    n_atoms = int()
    property_dict = defaultdict(float)
    with open(file_path, "r") as f:
        line_num = 0
        for line in f:
            if line_num == 0:
                n_atoms = int(line)
                line_num += 1
            elif line_num == 1:
                for k, v in properties_parser(header_regex, line):
                    if v == "-":
                        pass
                    else:
                        property_dict[k] = float(v)
                line_num += 1
            elif line_num < n_atoms + 2:
                elem_coord_items = properties_parser(COORD_RE, line)
                try:
                    for elem_coord, val in elem_coord_items:
                        elem_coords[elem_coord].append(val)
                except ValueError:
                    print("ValueError at {line} in {file_path}")
                line_num += 1
            else:
                print(f"{file_path} finished at line {line_num}.")
                break
    return name, n_atoms, elem_coords, property_dict


def reader_ccsd(file_path):
    name, n_atoms, elem_coords, properties = xyz_parser(
        file_path, CCSD_HEADER_RE
    )
    positions = list(zip(elem_coords["x"], elem_coords["y"], elem_coords["z"]))
    atoms = AtomicConfiguration(
        names=[name], symbols=elem_coords["element"], positions=positions
    )
    atoms.info["name"] = name
    atoms.info["n_atoms"] = n_atoms
    for key in properties.keys():
        atoms.info[key] = properties[key]
    return [atoms]


def reader_b3lyp(file_path):
    name, n_atoms, elem_coords, properties = xyz_parser(
        file_path, B3LYP_SCAN0_HEADER_RE
    )
    positions = list(zip(elem_coords["x"], elem_coords["y"], elem_coords["z"]))
    atoms = AtomicConfiguration(
        names=[name], symbols=elem_coords["element"], positions=positions
    )
    atoms.info["name"] = name
    atoms.info["n_atoms"] = n_atoms
    for key in properties.keys():
        atoms.info[key] = properties[key]
    return [atoms]


def load_data_wrapper(client, reader, glob_string, metadata, energy_map):
    configurations = load_data(
        # Data can be downloaded here:
        # 'https://archive.materialscloud.org/record/2019.0002/v3'
        file_path=DATASET_PATH,
        file_format="folder",
        name_field="name",
        elements=["C", "O", "H", "N", "S", "Cl"],
        reader=reader,
        glob_string=glob_string,
        generator=False,
    )
    if len(configurations) == 0:
        print(f"Check glob {glob_string}. No configurations found.")
        return [], []
    ids = list(
        client.insert_data(
            configurations,
            property_map=energy_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    return all_co_ids, all_do_ids


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

    # Metadata and property maps
    ccsd_metadata = {
        "software": {"value": "Psi4"},
        "method": {"value": "CCSD"},
    }
    ccsd_total_energy_map = {
        "potential-energy": [
            {
                "energy": {"field": "total_energy", "units": "a.u."},
                "per-atom": {"value": False, "units": None},
                "_metadata": ccsd_metadata,
            }
        ]
    }

    b3lyp_metadata = {
        "software": {"value": "Psi4"},
        "method": {"value": "DFT(B3LYP)"},
        "lumo-energy": {"field": "lumo_energy", "units": "a.u."},
        "homo-energy": {"field": "homo_energy", "units": "a.u."},
    }

    b3lyp_energy_map = {
        "potential-energy": [
            {
                "energy": {"field": "total_energy", "units": "a.u."},
                "per-atom": {"value": False, "units": None},
                "_metadata": b3lyp_metadata,
            }
        ]
    }

    scan0_metadata = {
        "software": {"value": "Q-Chem"},
        "method": {"value": "DFT(SCAN0)"},
    }

    client.insert_property_definition(potential_energy_pd)

    # Load data
    all_co_ids = set()
    all_do_ids = set()
    co_ids, do_ids = load_data_wrapper(
        client,
        reader_ccsd,
        "CCSD_daDZ/*.xyz",
        ccsd_metadata,
        ccsd_total_energy_map,
    )
    all_co_ids.update(co_ids)
    all_do_ids.update(do_ids)
    co_ids, do_ids = load_data_wrapper(
        client,
        reader_b3lyp,
        "B3LYP_daTZ/*xyz",
        b3lyp_metadata,
        b3lyp_energy_map,
    )
    all_co_ids.update(co_ids)
    all_do_ids.update(do_ids)
    co_ids, do_ids = load_data_wrapper(
        client,
        reader_b3lyp,
        "B3LYP_daDZ/*.xyz",
        b3lyp_metadata,
        b3lyp_energy_map,
    )
    all_co_ids.update(co_ids)
    all_do_ids.update(do_ids)
    co_ids, do_ids = load_data_wrapper(
        client,
        reader_b3lyp,
        "SCAN0_daDZ/*.xyz",
        scan0_metadata,
        b3lyp_energy_map,
    )
    all_co_ids.update(co_ids)
    all_do_ids.update(do_ids)

    # Create configuration sets
    cs_regexes = [
        # ["QM7b_AlphaML", ".*", "All QM7b and AlphaML configurations"],
        [
            "B3LYP_daTZ",
            "B3LYP_daTZ",
            "Configurations gathered using DFT B3LYP methods and the "
            "d-aug-cc-pVTZ basis set",
        ],
        [
            "B3LYP_daDZ",
            "B3LYP_daDZ",
            "Configurations gathered using DFT B3LYP methods and the "
            "d-aug-cc-pVDZ basis set",
        ],
        [
            "SCAN0",
            "SCAN0",
            "Configurations gathered using DFT SCAN0 methods and the "
            "d-aug-cc-pVDZ basis set",
        ],
        [
            "CCSD_daDZ",
            "CCSD_daDZ",
            "Configurations gathered using quantum calculation CCSD methods"
            " and the d-aug-cc-pVDZ basis set",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={
                "hash": {"$in": list(all_co_ids)},
                "names": {"$regex": regex},
            },
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        if len(co_ids) == 0:
            pass
        else:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)

    # Insert dataset
    client.insert_dataset(
        cs_ids=cs_ids,
        pr_hashes=list(all_do_ids),
        name="QM7b_AlphaML",
        authors=[
            "Yang Yang",
            "Ka Un Lao",
            "David M. Wilkins",
            "Andrea Grisafi",
            "Michele Ceriotti",
            "Robert A. DiStasio Jr",
        ],
        links=[
            "https://doi.org/10.24435/materialscloud:2019.0002/v3",
            "https://doi.org/10.1038/s41597-019-0157-8",
        ],
        description="Energy, computed with LR-CCSD, "
        "hybrid DFT (B3LYP & SCAN0) for 7211 molecules in QM7b and 52 "
        "molecules in AlphaML showcase database.",
        verbose=True,
    )


# Property group names for ccsd
# iso_di_pol
# aniso_di_pol
# di_pol_1
# di_pol_2
# di_pol_3
# di_pol_4
# di_pol_5
# di_pol_6
# di_moment_1
# di_moment_2
# di_moment_3
# quad_moment_1
# quad_moment_2
# quad_moment_3
# quad_moment_4
# quad_moment_5
# quad_moment_6
# total_energy
# same_spin_mp2_corr
# oppos_spin_mp2_corr
# same_spin_ccsd_corr
# oppos_spin_ccsd_corr

# Property group names for B3LYP and SCAN0
# iso_di_pol
# aniso_di_pol
# di_pol_1
# di_pol_2
# di_pol_3
# di_pol_4
# di_pol_5
# di_pol_6
# di_moment_1
# di_moment_2
# di_moment_3
# quad_moment_1
# quad_moment_2
# quad_moment_3
# quad_moment_4
# quad_moment_5
# quad_moment_6
# total_energy
# homo_energy
# lumo_energy

if __name__ == "__main__":
    main(sys.argv[1:])

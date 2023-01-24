from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration
from collections import defaultdict
from pathlib import Path
import re
import property_definitions_additional as pda

if __name__ == "__main__":

    client = MongoDatabase("test2", drop_database=True)

    # Change to location of dataset
    DATASET_FP = "/Users/piper/Code/colabfit/data/gdb9/test_set/"

    # Parsing and file reading functions
    HEADER_RE = re.compile(
        r"gdb (?P<index>\d+)\s(?P<rotational_a>[-\d\.]+)\s"
        r"(?P<rotational_b>[-\d\.]+)\s(?P<rotational_c>[-\d\.]+)\s"
        r"(?P<dipole_moment>[-\d\.]+)\s(?P<isotropic_polarizability>[-\d\.]+)"
        r"\s(?P<homo>[-\d\.]+)\s(?P<lumo>[-\d\.]+)\s"
        r"(?P<homo_lumo_gap>[-\d\.]+)\s(?P<elect_spatial_extent>[-\d\.]+)"
        r"\s(?P<zpve>[-\d\.]+)\s(?P<internal_energy_0>[-\d\.]+)\s"
        r"(?P<internal_energy_298>[-\d\.]+)\s(?P<enthalpy>[-\d\.]+)\s"
        r"(?P<free_energy>[-\d\.]+)\s(?P<heat_capacity>[-\d\.]+)"
    )

    COORD_RE = re.compile(
        r"(?P<element>[a-zA-Z]{1,2})\s+(?P<x>\S+)\s+"
        r"(?P<y>\S+)\s+(?P<z>\S+)\s+(?P<mulliken>\S+)"
    )

    def properties_parser(re_match, line):
        groups = re_match.match(line)
        return groups.groupdict().items()

    def xyz_parser(file_path, header_regex):
        file_path = Path(file_path)
        name = "gdb9_nature_2014"
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
                    if "*^" in line:
                        line = line.replace("*^", "e")
                    elem_coord_items = properties_parser(COORD_RE, line)
                    try:
                        for elem_coord, val in elem_coord_items:
                            elem_coords[elem_coord].append(val)
                    except ValueError:
                        print("ValueError at {line} in {file_path}")
                    line_num += 1
                else:
                    return name, n_atoms, elem_coords, property_dict

    def reader(file_path):
        name, n_atoms, elem_coords, properties = xyz_parser(
            file_path, HEADER_RE
        )
        positions = list(
            zip(elem_coords["x"], elem_coords["y"], elem_coords["z"])
        )
        atoms = AtomicConfiguration(
            names=[name], symbols=elem_coords["element"], positions=positions
        )
        atoms.info["name"] = name
        atoms.info["n_atoms"] = n_atoms
        for key in properties.keys():
            atoms.info[key] = properties[key]
        return [atoms]

    # Load configurations
    configurations = load_data(
        # Data can be downloaded here:
        # 'https://doi.org/10.6084/m9.figshare.c.978904.v5'
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N", "F"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    pds = [
        pda.free_energy_pd,
        pda.dipole_moment_pd,
        pda.electronic_spatial_extent_pd,
        pda.enthalpy_pd,
        pda.homo_energy_pd,
        pda.lumo_energy_pd,
        pda.polarizability_pd,
        pda.homo_lumo_gap_pd,
        pda.internal_energy_pd,
        pda.zpve_pd,
        pda.heat_capacity_pd,
    ]
    for pd in pds:
        client.insert_property_definition(pd)
    metadata = {
        "software": {"value": ["MOPAC", "Gaussian 09"]},
        "method": {"value": ["DFT", "B3LYP", "6-31G(2df,p)"]},
    }
    property_map_1 = {
        "free-energy": [
            {
                "energy": {"field": "free_energy", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 298.25, "units": "K"},
                "_metadata": metadata,
            }
        ],
        "dipole-moment": [
            {
                "dipole-moment": {"field": "dipole_moment", "units": "Debye"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "lumo-energy": [
            {
                "energy": {"field": "lumo", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "homo-energy": [
            {
                "energy": {"field": "homo", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "homo-lumo-gap": [
            {
                "homo-lumo-gap": {"field": "homo_lumo_gap", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "polarizability": [
            {
                "polarizability": {
                    "field": "polarizability",
                    "units": "Bohr^3",
                },
                "iso-aniso": {"value": "isotropic", "units": None},
                "di-quad": {"value": "dipole", "units": None},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "electronic-spatial-extent": [
            {
                "electronic-spatial-extent": {
                    "field": "elec_spatial_extent",
                    "units": "Bohr^2",
                },
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "zpve": [
            {
                "zpve": {"field": "zpve", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "internal-energy": [
            {
                "energy": {"field": "internal_energy_0", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 0, "units": "K"},
                "_metadata": metadata,
            }
        ],
        "enthalpy": [
            {
                "enthalpy": {"field": "enthalpy", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 298.15, "units": "K"},
                "_metadata": metadata,
            }
        ],
        "heat-capacity": [
            {
                "heat-capacity": {
                    "field": "heat-capacity",
                    "units": "cal/(mol K)",
                },
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 298, "units": "K"},
                "_metadata": metadata,
            }
        ],
    }

    property_map_2 = {
        "internal-energy": [
            {
                "energy": {"field": "internal_energy_298", "units": "Ha"},
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 298, "units": "K"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map_1,
            generator=False,
            verbose=True,
        )
    )
    client.insert_data(
        configurations,
        property_map=property_map_2,
        generator=False,
        verbose=True,
    )
    all_co_ids, all_do_ids = list(zip(*ids))
    hashes = client.get_data("configurations", fields=["hash"])
    name = "GDB_9"
    cs_ids = []
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": hashes}},
        ravel=True,
    ).tolist()

    print(
        "Configuration set ", f"({name}):".rjust(22), f"{len(co_ids)}".rjust(7)
    )

    cs_id = client.insert_configuration_set(
        co_ids,
        description="GDB-9 dataset,"
        " from GDB_9_nature_2014, a subset of GDB-17",
        name=name,
    )

    cs_ids.append(cs_id)

    ds_id = client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="GDB_9_nature_2014",
        authors=["R. Ramakrishnan, P.O. Dral, M. Rupp, O.A. von Lilienfeld"],
        links=[
            "https://doi.org/10.6084/m9.figshare.c.978904.v5",
            "https://doi.org/10.1038/sdata.2014.22",
        ],
        description="133,855 of stable small organic molecules composed of "
        "CHONF. A subset of GDB-17, with calculations of energies, dipole "
        "moment, polarizability and enthalpy. Calculations performed at "
        "B3LYP/6-31G(2df,p) level of theory",
        verbose=True,
    )

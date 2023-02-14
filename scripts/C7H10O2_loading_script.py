from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration
from collections import defaultdict
from pathlib import Path
import re

if __name__ == "__main__":
    # Importing additional property definitions defined locally
    # Ask GW if these are not present
    import property_definitions_additional as pda

    client = MongoDatabase("test4_e2e", drop_database=True)
    # Create custom regex parser for header and coordinate rows
    parser_match = re.compile(
        r"gdb (?P<index>\d+)\s(?P<rotational_a>-?\d+\.(\d+)?)\s"
        r"(?P<rotational_b>-?\d+\.(\d+)?)\s(?P<rotational_c>-?\d+\.(\d+)?)\s"
        r"(?P<mu>-?\d+\.(\d+)?)\s(?P<alpha>-?\d+\.(\d+)?)\s"
        r"(?P<homo>-?\d+\.(\d+)?)\s(?P<lumo>-?\d+\.(\d+)?)"
        r"\s(?P<gap>-?\d+\.(\d+)?)\s(?P<r2>-?\d+\.(\d+)?)"
        r"\s(?P<zpve>-?\d+\.(\d+)?)\s(?P<u0>-?\d+\.(\d+)?)\s"
        r"(?P<u>-?\d+\.(\d+)?)\s(?P<h>-?\d+\.(\d+)?)\s"
        r"(?P<g>-?\d+\.(\d+)?)\s(?P<cv>-?\d+\.(\d+)?)"
    )
    # format to match 'gdb 1	3.1580357	1.2436329	1.1060216	1.1312	77.92	-0.21236	0.02396	0.23632	1176.6995	0.155411	-422.593067	-422.583794	-422.58285	-422.62703	34.695'
    # The numeric values on the xyz comment line indicate the following in this
    # order, according to the README:

    # index     rotational_a    rotational_b    rotational_c
    # [dipole movement (mu)]      [isotropic polarizability (alpha)]
    # homo      lumo    gap     [electronic spatial extent (r2)]    zpve
    # [internal energy at 0K (u0)]
    # [internal energy at 298K (u)]     [Enthalpy (h)]   [free energy (g)]
    # [heat capacity (cv)]

    coord_match = re.compile(
        r"(?P<element>[a-zA-Z]{1,2})\s+(?P<x>\S+)\s+"
        r"(?P<y>\S+)\s+(?P<z>\S+)\s+(?P<mulliken>\S+)"
    )
    # Format to match(spaces are tabs, no beginning space):
    # C	 0.3852095134	 0.5870284364	-0.7883677644	-0.178412
    # Values represent element [x y z coordinates] partial charge (Mulliken)

    def properties_parser(line):
        groups = parser_match.match(line)
        return groups.groupdict()

    # Create functions to run file and heading parsers
    def xyz_parser(file_path):
        file_path = Path(file_path)
        name = file_path.stem
        elem_coords = defaultdict(list)
        n_atoms = int()
        with open(file_path, "r") as f:
            line_num = 0
            for line in f:
                if line_num == 0:
                    n_atoms = int(line)
                    line_num += 1
                elif line_num == 1:
                    property_dict = {
                        k: float(v) for k, v in properties_parser(line).items()
                    }
                    line_num += 1
                elif line_num < n_atoms + 2:
                    if "*^" in line:
                        line = line.replace("*^", "e")
                    groups = coord_match.match(line)
                    try:
                        for elem_coord, val in groups.groupdict().items():
                            elem_coords[elem_coord].append(val)
                    except ValueError:
                        print("ValueError at {line} in {file_path}")
                    line_num += 1
                elif line_num >= n_atoms + 2:
                    return name, n_atoms, elem_coords, property_dict
                else:
                    print(f"{file_path} finished at line {line_num}.")
                    break

    def reader(file_path):
        name, n_atoms, elem_coords, properties = xyz_parser(file_path)
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
        file_path="/Users/piper/Code/colabfit/data/C7O2H10_nsd_2014/",
        file_format="folder",
        name_field="name",
        elements=["C", "O", "H"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    # Loaded from a local file of definitions

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

    # Define properties
    metadata = {
        "software": {"value": ["MOPAC", "Gaussian 09"]},
        "method": {"value": "G4MP2"},
    }
    property_map = {
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
                "_metadata": metadata,
            }
        ],
        "lumo-energy": [
            {
                "energy": {"field": "lumo", "units": "Ha"},
                "_metadata": metadata,
            }
        ],
        "homo-energy": [
            {
                "energy": {"field": "homo", "units": "Ha"},
                "_metadata": metadata,
            }
        ],
        "homo-lumo-gap": [
            {
                "homo-lumo-gap": {"field": "gap", "units": "Ha"},
                "_metadata": metadata,
            }
        ],
        "polarizability": [
            {
                "polarizability": {
                    "field": "polarizability",
                    "units": "Bohr^3",
                    "di-quad": {"value": "dipole", "units": None},
                    "iso-aniso": {"value": "isotropic", "units": None},
                },
                "_metadata": metadata,
            }
        ],
        "electronic-spatial-extent": [
            {
                "electronic-spatial-extent": {
                    "field": "spatial_extent",
                    "units": "Bohr^2",
                },
                "_metadata": metadata,
            }
        ],
        "zpve": [
            {
                "zpve": {"field": "zpve", "units": "Ha"},
                "_metadata": metadata,
            }
        ],
        "internal-energy": [
            {
                "energy": {
                    "field": "internal_energy_0",
                    "units": "Ha",
                },
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 0, "units": "K"},
                "_metadata": metadata,
            }
        ],
        "enthalpy": [
            {
                "enthalpy": {"field": "enthalpy", "units": "Ha"},
                "temperature": {"value": 298.15, "units": "K"},
                "_metadata": metadata,
            }
        ],
        "heat-capacity": [
            {
                "heat-capacity": {
                    "field": "heat-capacity",
                    "units": "cal/[mol K]",
                },
                "temperature": {"value": 298, "units": "K"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    # Add second set of internal energy properties with different
    # temperature value
    property_map2 = {
        "internal-energy": [
            {
                "internal-energy": {
                    "field": "internal_energy_298",
                    "units": "Ha",
                },
                "per-atom": {"value": False, "units": None},
                "temperature": {"value": 298.15, "units": "K"},
                "_metadata": metadata,
            }
        ]
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map2,
            generator=False,
            verbose=True,
        )
    )
    # Name according to spreadsheet, not according to ReadMe file/filenames
    name = "C7H10O2"
    cs_ids = []
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": all_co_ids}},
        ravel=True,
    ).tolist()

    print(
        "Configuration set", "({name}):".rjust(22), f"{len(co_ids)}".rjust(7)
    )

    cs_id = client.insert_configuration_set(
        co_ids, description=f"Set from dataset {name}", name=name
    )

    cs_ids.append(cs_id)

    ds_id = client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="COH_SD_2014",
        authors=["R. Ramakrishnan, P.O. Dral, M. Rupp, O.A. von Lilienfeld"],
        links=["https://doi.org/10.6084/m9.figshare.c.978904.v5"],
        description="6095 isomers of C7O2H10, energetics were calculated"
        " at the G4MP2 level of theory",
        verbose=True,
    )

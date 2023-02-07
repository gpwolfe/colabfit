from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

elements = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
]

DATASET_FP = Path("data/mat_proj")


def reader(file_path):
    atom = read(file_path)
    yield atom


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=elements,
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    for pd in [
        potential_energy_pd,
        atomic_forces_pd,
        cauchy_stress_pd,
        free_energy_pd,
    ]:
        client.insert_property_definition(pd)

    metadata = {
        "software": {"value": "Amsterdam Modeling Suite"},
        "method": {"field": "calc_type"},
        "entropy": {"field": "e_wo_entrp"},
        "material-id": {"field": "material_id"},
        "zpe": {"field": "e_0_energy"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "e_fr_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV/A"},
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
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": all_co_ids}},
        ravel=True,
    ).tolist()

    desc = "Materials Project dataset"
    cs_ids = []
    cs_id = client.insert_configuration_set(
        co_ids, description=desc, name="materials_project"
    )
    cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="Materials Project",
        authors=[
            "A. Jain, S.P. Ong, G. Hautier, W. Chen, W.D. Richards, S. Dacek,"
            " S. Cholia, D. Gunter, D. Skinner, G. Ceder, K.A. Persson"
        ],
        links=[
            "https://materialsproject.org/",
        ],
        description="Configurations from the Materials Project database:"
        " an online resource with the goal of computing properties of all"
        " inorganic materials.",
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

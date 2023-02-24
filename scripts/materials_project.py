from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("/home/gpwolfe/colabfit/mp_xyz_files_error_124s")

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


def reader(file_path):
    atom = read(file_path, index=":")
    return atom


def main(ip, fileset: list, dataset_id=None, config_set_id=None):
    client = MongoDatabase("----", uri=f"mongodb://{ip}:27017")
    configurations = []
    for path in fileset:
        configs = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=elements,
            reader=reader,
            glob_string=path.name,
            generator=False,
        )
        configurations += configs

    # Skip if dataset has already been created
    if dataset_id is None:
        for pd in [
            atomic_forces_pd,
            cauchy_stress_pd,
            free_energy_pd,
        ]:
            client.insert_property_definition(pd)

    metadata = {
        "software": {"value": "VASP"},
        "method": {"field": "calc_type"},
        "free_energy_no_entropy": {"field": "e_wo_entrp"},
        "material-id": {"field": "material_id"},
        "internal_energy": {"field": "e_0_energy"},
    }
    property_map = {
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

    # Create a dataset/config set if none exists, otherwise reuse MP dataset
    # Currently, update_dataset requires configurations sets to be updated
    # as well.
    if dataset_id is None:
        desc = "Materials Project dataset"
        cs_ids = []
        config_set_id = client.insert_configuration_set(
            co_ids, description=desc, name="materials_project"
        )
        cs_ids.append(config_set_id)

        dataset_id = client.insert_dataset(
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
        return dataset_id, config_set_id
    else:
        config_set_id = client.update_configuration_set(
            cs_id=config_set_id, add_ids=all_co_ids
        )
        dataset_id = client.update_dataset(
            ds_id=dataset_id, add_cs_ids=config_set_id, add_do_ids=all_do_ids
        )
    return dataset_id, config_set_id


if __name__ == "__main__":
    batch_size = 50
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(sys.argv[1:])
    ip = args.ip

    files = list(DATASET_FP.glob("*.xyz"))

    # Import by batch, with first batch returning dataset-id and config-set-id
    n_batches = len(files) // batch_size
    batch_1 = files[:batch_size]
    dataset_id, config_set_id = main(ip, batch_1, None, None)
    print(dataset_id)
    for n in range(1, n_batches):
        batch_n = files[batch_size * n : batch_size * (n + 1)]
        dataset_id, config_set_id = main(
            ip, batch_n, dataset_id=dataset_id, config_set_id=config_set_id
        )
    if len(files) % batch_size:
        batch_n = files[batch_size * n_batches :]
        dataset_id, config_set_id = main(
            ip, batch_n, dataset_id, config_set_id
        )

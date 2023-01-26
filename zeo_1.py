"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2021.171
Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""

from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
    cauchy_stress_pd,
)
import numpy as np
from pathlib import Path


def main():
    DATASET_FP = Path("/Users/piper/Code/colabfit/data/zeo-1/npz/")
    client = MongoDatabase("test", drop_database=True)

    def reader(file):
        npz = np.load(file)
        name = file.stem
        atoms = []
        for xyz, lattice, energy, stress, gradients, charges in zip(
            npz["xyz"],
            npz["lattice"],
            npz["energy"],
            npz["stress"],
            npz["gradients"],
            npz["charges"],
        ):
            atoms.append(
                Atoms(
                    numbers=npz["numbers"],
                    positions=xyz,
                    cell=lattice,
                    pbc=True,
                    info={
                        "name": name,
                        "potential_energy": energy,
                        "cauchy_stress": stress,
                        "nuclear_gradients": gradients,
                        "partial_charges": charges,
                    },
                )
            )
        return atoms

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=[
            "O",
            "Si",
            "Ge",
            "Li",
            "H",
            "Al",
            "K",
            "Ca",
            "C",
            "N",
            "Na",
            "F",
            "Ba",
            "Cs",
            "Be",
        ],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)
    metadata = {
        "software": {"value": "Amsterdam Modeling Suite"},
        "method": {"value": "revPBE"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "a.u."},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "cauchy_stress", "units": "a.u."},
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

    desc = "All configurations from Zeo-1 dataset"
    cs_ids = []
    cs_id = client.insert_configuration_set(
        co_ids, description=desc, name="Zeo-1"
    )
    cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="Zeo-1_sd_2022",
        authors=["A. Christensen, O. A. von Lilienfeld"],
        links=[
            "https://archive.materialscloud.org/record/2021.171",
            "https://www.nature.com/articles/s41597-022-01160-5",
        ],
        description="130,000 configurations of zeolite from the "
        "Database of Zeolite Structures. Calculations performed using "
        "Amsterdam Modeling Suite software.",
        verbose=True,
    )


if __name__ == "__main__":
    main()

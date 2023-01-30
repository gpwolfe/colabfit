"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.24435/materialscloud:14-4m
Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd
import numpy as np
from pathlib import Path


def main():

    DATASET_FP = Path("/Users/piper/Code/colabfit/data/gfn-xtb-si/npz/")
    client = MongoDatabase("test", drop_database=True)

    def reader(file):
        npz = np.load(file)
        name = file.stem
        atoms = []
        for xyz, energy, gradients in zip(
            npz["xyz"], npz["energy"], npz["gradients"]
        ):
            atoms.append(
                Atoms(
                    numbers=npz["numbers"],
                    positions=xyz,
                    pbc=False,
                    info={
                        "name": name,
                        "potential_energy": energy,
                        "nuclear_gradients": gradients,
                    },
                )
            )
        return atoms

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["O", "Si", "C", "H", "N", "Cl", "S", "F", "P", "Br"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": "Amsterdam Modeling Suite"},
        "method": {"value": "revPBE"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "potential_energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
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

    desc = "All configurations from GFN-xTB dataset"
    cs_ids = []
    cs_id = client.insert_configuration_set(
        co_ids, description=desc, name="GFN-xTB"
    )
    cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="GFN-xTB_jcim_2021",
        authors=["L. Komissarov, T. Verstraelen"],
        links=[
            "https://doi.org/10.24435/materialscloud:14-4m",
            "https://doi.org/10.1021/acs.jcim.1c01170",
        ],
        description="10,000 configurations of organosilicon compounds "
        "with energies predicted by an improved GFN-xTB Hamiltonian "
        "parameterization, using revPBE.",
        verbose=True,
    )


if __name__ == "__main__":
    main()

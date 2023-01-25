"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.6084/m9.figshare.12672038.v3
Change DATASET_FP to reflect location of parent folder
Change database name as appropriate
"""
from ase import Atoms
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
)
import numpy as np
from pathlib import Path


def main():

    DATASET_FP = Path("/Users/piper/Code/colabfit/data/rmd17")
    client = MongoDatabase("test", drop_database=True)

    def reader(file):
        atoms = []
        with np.load(file) as npz:
            npz = np.load(file)
            for coords, energy, forces, md17_index in zip(
                npz["coords"],
                npz["energies"],
                npz["forces"],
                npz["old_indices"],
            ):
                atoms.append(
                    Atoms(
                        numbers=npz["nuclear_charges"],
                        positions=coords,
                        info={
                            "name": file.stem,
                            "energy": energy,
                            "forces": forces,
                            "md17_index": md17_index,
                        },
                    )
                )
        return atoms

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["C", "H", "O", "N"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": "ORCA"},
        "method": {"value": "DFT-PBE def2-SVP"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "kcal/mol"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "kcal/mol/angstrom"},
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
    cs_regexes = [
        ["rmd17_configurations", ".*", "All rmd17 configurations"],
        ["aspirin", "aspirin", "All aspirin rmd17 configurations"],
        ["azobenzene", "azobenzene", "All azobenzene rmd17 configurations"],
        ["benzene", "benzene", "All benzene rmd17 configurations"],
        ["ethanol", "ethanol", "All ethanol rmd17 configurations"],
        [
            "malonaldehyde",
            "malonaldehyde",
            "All malonaldehyde rmd17 configurations",
        ],
        ["naphthalene", "naphthalene", "All naphthalene rmd17 configurations"],
        ["paracetamol", "paracetamol", "All paracetamol rmd17 configurations"],
        ["salicylic", "salicylic", "All salicylic rmd17 configurations"],
        ["toluene", "toluene", "All toluene rmd17 configurations"],
        ["uracil", "uracil", "All uracil rmd17 configurations"],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(22),
            f"{len(co_ids)}".rjust(7),
        )

        if len(co_ids) > 0:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)
    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="rMD17",
        authors=["A. Christensen, O. A. von Lilienfeld"],
        links=[
            "https://doi.org/10.6084/m9.figshare.12672038.v3",
            "https://doi.org/10.48550/arXiv.2007.09593",
        ],
        description="A dataset of 10 molecules (aspirin, "
        "azobenzene, benzene, ethanol, malonaldehyde, naphthalene, "
        "paracetamol, salicylic, toluene, uracil) with 100,000 structures"
        "calculated for each at the PBE/def2-SVP level of theory, using ORCA.",
        verbose=True,
    )


if __name__ == "__main__":
    main()

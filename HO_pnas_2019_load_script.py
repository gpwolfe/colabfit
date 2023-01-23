from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    # total energy not yet implemented in colabfit module
    # total_energy_pd,
    atomic_forces_pd,
)
import ase

# temporary import until property definitions implemented in colabfit module
import property_definitions_additional as pda

if "__name__" == "__main__":
    client = MongoDatabase("test", drop_database=True)

    def reader(file_path):
        file_name = file_path.stem
        atoms = ase.io.read(file_path, index=":")
        for atom in atoms:
            atom.info["name"] = file_name
        return atoms

    configurations = load_data(
        # Data can be downloaded here:
        # https://archive.materialscloud.org/record/2018.0020/v1
        file_path="/Users/piper/Code/colabfit/data/liquid_solid_water/",
        file_format="folder",
        name_field="name",
        elements=["H", "O"],
        reader=reader,
        glob_string="*.xyz",
        generator=False,
    )
    # Load from colabfit's definitions
    client.insert_property_definition(pda.total_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    metadata = {
        "software": {"value": ["LAMMPS", "i-PI"]},
        "method": {"value": ["revPBE0-D3", "DFT"]},
    }
    property_map = {
        "total-energy": [
            {
                "energy": {"field": "TotEnergy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/Ang"},
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
    hashes = client.get_data("configurations", fields=["hash"])
    name = "HO_pnas_2019"
    cs_ids = []
    co_ids = client.get_data(
        "configurations",
        fields="hash",
        query={"hash": {"$in": hashes}},
        ravel=True,
    ).tolist()

    print(
        "Configuration set ",
        f"({name}):".rjust(22),
        f"{len(co_ids)}".rjust(7),
    )

    cs_id = client.insert_configuration_set(
        co_ids,
        description="Liquid and solid H2O/water thermodynamics",
        name=name,
    )

    cs_ids.append(cs_id)
    ds_id = client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="HO_pnas_2019",
        authors=["B. Cheng, E. Engel, J. Behler, C. Dellago, M. Ceriotti"],
        links=[
            "https://archive.materialscloud.org/record/2018.0020/v1",
            "https://www.pnas.org/doi/full/10.1073/pnas.1815117116",
        ],
        description="1590 configurations of H2O/water "
        "with total energy and forces calculated using "
        "a hybrid approach, DFT and revPBE0-D3 ",
        verbose=True,
    )

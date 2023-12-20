"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------
columns
[
    "phase",
    "supercell",
    "subst",
    "index",
    "weight",
    "dopant",
    "space_group_number",
    # "formula",
    # "natoms",
    "atomic_numbers",
    # "nelements",
    "cell",
    "pos",
    "relaxed_cell_DFT",
    "relaxed_pos_DFT",
    "relaxed_pressure_DFT",
    "relaxed_forces_DFT",
    "relaxed_energy_DFT",
    "relaxed_energy_pa_DFT",
    "formation_energy_pa_DFT",
    "val_0_DFT",
    "val_1_DFT",
    "val_2_DFT",
    "val_3_DFT",
    "val_4_DFT",
    "val_5_DFT",
    "val_6_DFT",
    "val_7_DFT",
    "val_8_DFT",
    "val_9_DFT",
    "val_10_DFT",
    "val_11_DFT",
    "val_12_DFT",
    "val_13_DFT",
    "val_14_DFT",
    "val_15_DFT",
    "val_16_DFT",
    "val_17_DFT",
    "val_18_DFT",
    "val_19_DFT",
    "val_20_DFT",
    "val_21_DFT",
    "val_22_DFT",
    "val_23_DFT",
    "val_24_DFT",
    "val_25_DFT",
    "val_26_DFT",
    "val_27_DFT",
    "val_28_DFT",
    "val_29_DFT",
    "val_30_DFT",
    "val_31_DFT",
    "val_32_DFT",
    "val_33_DFT",
    "val_34_DFT",
    "val_35_DFT",
    "val_36_DFT",
    "val_37_DFT",
    "val_38_DFT",
    "val_39_DFT",
    "val_40_DFT",
    "val_41_DFT",
    "val_42_DFT",
    "val_43_DFT",
    "val_44_DFT",
    "val_45_DFT",
    "val_46_DFT",
    "val_47_DFT",
    "inWhichPart",
]"""


from argparse import ArgumentParser
from functools import partial
import json
from pathlib import Path
import sys

from ase.atoms import Atoms
import numpy as np

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/doped_CsPbI3_energetics")
DATASET_NAME = "doped_CsPbI3_energetics"
LICENSE = "https://opensource.org/licenses/MIT"

PUBLICATION = "https://doi.org/10.1016/j.commatsci.2023.112672"
DATA_LINK = "https://github.com/AIRI-Institute/doped_CsPbI3_energetics"
# OTHER_LINKS = []

AUTHORS = [
    "Roman A. Eremin",
    "Innokentiy S. Humonen",
    "Alexey A. Kazakov, Vladimir D. Lazarev",
    "Anatoly P. Pushkarev",
    "Semen A. Budennyy",
]
DATASET_DESC = (
    "This dataset was created to explore the effect of "
    "Cd and Pb substitutions on the structural stability of inorganic lead halide "
    "perovskite CsPbI3. CsPbI3 undergoes a direct to indirect band-gap phase "
    "transition at room temperature. The dataset contains configurations of "
    "CsPbI3 with low levels of Cd and Zn, which were used to train a GNN model "
    "to predict the energetics of structures with higher levels of substitutions."
)
ELEMENTS = None
GLOB_STR = "*_data.pkl"

PI_METADATA = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT-PBE"},
    "input": {
        "value": {
            "ENCUT": {"value": 600, "units": "eV"},
            "kpoint-scheme": "gamma-centered",
            "EDIFFG": 0.01,
            "EDIFF": 10e-5,
        }
    },
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA,
        },
    ],
    "formation-energy": [
        {
            "energy": {"field": "form_energy", "units": "eV"},
            "per-atom": {"value": True, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
}

CO_METADATA = {
    "phase": {"field": "phase"},
    "supercell": {"field": "supercell"},
    "num_substites": {"field": "num_substites"},
    "dopant": {"field": "dopant"},
    "space_group_number": {"field": "space_group_number"},
    "relaxed_pressure": {"field": "relaxed_pressure", "units": "kbar"},
    "relaxed_cell": {"field": "relaxed_cell"},
    "relaxed_positions": {"field": "relaxed_positions"},
}


DSS = [
    (
        "doped_CsPbI3_energetics_test",
        "test",
        "The test set from the doped CsPbI3 energetics dataset. " + DATASET_DESC,
    ),
    (
        "doped_CsPbI3_energetics_train_validate",
        "tr_val",
        "The training + validation set from the doped CsPbI3 energetics dataset. "
        + DATASET_DESC,
    ),
    # The inference split doesn't have any energetics data (ie. no DFT data)
    # (
    #     "doped_CsPbI3_energetics_inference",
    #     "inference",
    #     "The predicted structures and energetics with higher Cd/Zn substitution "
    #     "levels from the doped CsPbI3 energetics dataset, generated using a GNN "
    #     "trained on structures with lower substitution levels. " + DATASET_DESC,
    # ),
]


def reader(part, fp):
    data = np.load(fp, allow_pickle=True)
    data = data[data["inWhichPart"] == part]
    for i, (ix, row) in enumerate(data.iterrows()):
        info = dict()
        is_val = []
        for column in [
            "val_0_DFT",
            "val_1_DFT",
            "val_2_DFT",
            "val_3_DFT",
            "val_4_DFT",
            "val_5_DFT",
            "val_6_DFT",
            "val_7_DFT",
            "val_8_DFT",
            "val_9_DFT",
            "val_10_DFT",
            "val_11_DFT",
            "val_12_DFT",
            "val_13_DFT",
            "val_14_DFT",
            "val_15_DFT",
            "val_16_DFT",
            "val_17_DFT",
            "val_18_DFT",
            "val_19_DFT",
            "val_20_DFT",
            "val_21_DFT",
            "val_22_DFT",
            "val_23_DFT",
            "val_24_DFT",
            "val_25_DFT",
            "val_26_DFT",
            "val_27_DFT",
            "val_28_DFT",
            "val_29_DFT",
            "val_30_DFT",
            "val_31_DFT",
            "val_32_DFT",
            "val_33_DFT",
            "val_34_DFT",
            "val_35_DFT",
            "val_36_DFT",
            "val_37_DFT",
            "val_38_DFT",
            "val_39_DFT",
            "val_40_DFT",
            "val_41_DFT",
            "val_42_DFT",
            "val_43_DFT",
            "val_44_DFT",
            "val_45_DFT",
            "val_46_DFT",
            "val_47_DFT",
        ]:
            if row[column] is True:
                is_val.append(column.split("_")[1])
            info["in_validation_for_model_nums"] = is_val
        info["name"] = f"{ix}_{row['index']}"
        info["phase"] = row["phase"]
        info["supercell"] = row["supercell"]
        info["num_substites"] = row["subst"]
        info["dopant"] = row["dopant"]
        info["space_group_number"] = row["space_group_number"]
        info["relaxed_pressure"] = row["relaxed_pressure_DFT"]
        info["forces"] = row["relaxed_forces_DFT"]
        info["energy"] = row["relaxed_energy_DFT"]
        info["form_energy"] = row["formation_energy_pa_DFT"]
        info["relaxed_cell"] = row["relaxed_cell_DFT"]
        info["relaxed_positions"] = row["relaxed_pos_DFT"]

        config = Atoms(
            positions=row["pos"], numbers=row["atomic_numbers"], cell=row["cell"]
        )
        config.info = info
        yield config


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    with open("formation_energy.json", "r") as f:
        formation_energy_pd = json.load(f)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(formation_energy_pd)

    for i, (ds_name, ds_split, ds_desc) in enumerate(DSS):
        ds_id = generate_ds_id()
        split_reader = partial(reader, ds_split)
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=split_reader,
            glob_string=GLOB_STR,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                co_md_map=CO_METADATA,
                property_map=PROPERTY_MAP,
                generator=False,
                verbose=False,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        css = [
            [
                f"{ds_name}_black_band_gap",
                {"names": {"$regex": "black"}},
                f"Configurations of CsPbI3 from {ds_name} dataset with black (direct) "
                "band gap phase",
            ],
            [
                f"{ds_name}_yellow_band_gap",
                {"names": {"$regex": "yellow"}},
                f"Configurations of CsPbI3 from {ds_name} dataset with black (direct) "
                "band gap phase",
            ],
            [
                f"{ds_name}_Zn-doped",
                {"names": {"$regex": "Zn"}},
                f"Configurations of CsPbI3 from {ds_name} dataset doped with Zn",
            ],
            [
                f"{ds_name}_Cd-doped",
                {"names": {"$regex": "Cd"}},
                f"Configurations of CsPbI3 from {ds_name} dataset doped with Cd",
            ],
        ]
        cs_ids = []
        for i, (name, query, desc) in enumerate(css):
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=name,
                description=desc,
                query=query,
            )

            cs_ids.append(cs_id)

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
            description=ds_desc,
            verbose=False,
            cs_ids=cs_ids,  # remove line if no configuration sets to insert
            data_license=LICENSE,
        )


if __name__ == "__main__":
    main(sys.argv[1:])


"""
columns
[
    "phase",
    "supercell",
    "subst",
    "index",
    "weight",
    "dopant",
    "space_group_number",
    # "formula",
    # "natoms",
    "atomic_numbers",
    # "nelements",
    "cell",
    "pos",
    "relaxed_cell_DFT",
    "relaxed_pos_DFT",
    "relaxed_pressure_DFT",
    "relaxed_forces_DFT",
    "relaxed_energy_DFT",
    "relaxed_energy_pa_DFT",
    "formation_energy_pa_DFT",
    "val_0_DFT",
    "val_1_DFT",
    "val_2_DFT",
    "val_3_DFT",
    "val_4_DFT",
    "val_5_DFT",
    "val_6_DFT",
    "val_7_DFT",
    "val_8_DFT",
    "val_9_DFT",
    "val_10_DFT",
    "val_11_DFT",
    "val_12_DFT",
    "val_13_DFT",
    "val_14_DFT",
    "val_15_DFT",
    "val_16_DFT",
    "val_17_DFT",
    "val_18_DFT",
    "val_19_DFT",
    "val_20_DFT",
    "val_21_DFT",
    "val_22_DFT",
    "val_23_DFT",
    "val_24_DFT",
    "val_25_DFT",
    "val_26_DFT",
    "val_27_DFT",
    "val_28_DFT",
    "val_29_DFT",
    "val_30_DFT",
    "val_31_DFT",
    "val_32_DFT",
    "val_33_DFT",
    "val_34_DFT",
    "val_35_DFT",
    "val_36_DFT",
    "val_37_DFT",
    "val_38_DFT",
    "val_39_DFT",
    "val_40_DFT",
    "val_41_DFT",
    "val_42_DFT",
    "val_43_DFT",
    "val_44_DFT",
    "val_45_DFT",
    "val_46_DFT",
    "val_47_DFT",
    "inWhichPart",
]"""

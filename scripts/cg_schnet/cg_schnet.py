"""
author:gpwolfe

Data can be downloaded from:

Download link:


Extract to project folder
tar -xf example.tar -C  <project_dir>/data/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy (as U0 energy)

Other properties added to metadata
----------------------------------
isotropic polarizability
relative atomic energy
(compensated) U0 energy
HOMO-LUMO gap


File notes
----------

"""
from argparse import ArgumentParser
from ase.db import connect
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd
from pathlib import Path
import sys

DATASET_FP = Path.cwd()
DATASET = "cG-SchNet"

SOFTWARE = "ORCA, SchNet"
METHODS = "DFT"
LINKS = [
    "https://doi.org/10.1038/s41467-022-28526-y",
    "https://github.com/atomistic-machine-learning/cG-SchNet/",
    "https://www.nature.com/articles/s41467-022-28526-y#data-availability",
]
AUTHORS = (
    "N.W.A. Gebauer, M. Gastegger, S.S.P. Hessmann, K.-R. Müller, K.T. Schütt"
)
DS_DESC = "Configurations from a cG-SchNet trained on a subset of the QM9\
 dataset. Model was trained with the intention of providing molecules with\
 specified functional groups or motifs, relying on sampling of molecular\
 fingerprint data. Relaxation data for the generated molecules is computed\
 using ORCA software. Configuration sets include raw data from\
 cG-SchNet-generated configurations, with models trained on several different\
 types of target data and DFT relaxation data as a separate configuration\
 set. Includes approximately 80,000 configurations."
ELEMENTS = ["C", "H", "O", "N", "F"]
GLOB_STR = "*.db"

COMP_PRED = {"relaxed": "computed", "generated": "predicted"}
KNOWN = {0: "novel isomer", 3: "novel stereo-isomer", 6: "unseen isomer"}


def reader(filepath):
    db = list(connect(filepath).select())
    # calculated or predicted, according to file name
    compd_or_predctd = COMP_PRED[filepath.stem.split("_")[-1]]
    configs = []
    info = dict()
    for i, row in enumerate(db):
        config = AtomicConfiguration(
            positions=row.positions,
            symbols=row.symbols,
            cell=row.cell,
            pbc=row.pbc,
        )
        info["0_energy"] = row.data.get(
            "computed_energy_U0", row.data.get("predicted_energy_U0")
        )
        info["relative-atomic-energy"] = row.data.get(
            "predicted_relative_atomic_energy",
            row.data.get("computed_relative_atomic_energy"),
        )
        info["computed_energy_U0_uncompensated"] = row.data.get(
            "computed_energy_U0_uncompensated"
        )

        info["gap"] = row.data.get(
            "predicted_gap", row.data.get("computed_gap")
        )
        info["isotropic-polarizability"] = row.data.get(
            "predicted_isotropic_polarizability",
            row.data.get("computed_isotropic_polarizability"),
        )
        info["target-tanimoto-similarity"] = row.data.get(
            "target_tanimoto_similarity"
        )
        info["changed"] = row.data.get("changed")
        info["equals"] = row.data.get("equals")
        info["known"] = KNOWN.get(
            row.data.get("known_relaxed"), KNOWN.get(row.data.get("known"))
        )
        info["gen_idx"] = row.data.get("gen_idx")
        info["rmsd"] = row.data.get("rmsd")
        info["computed-or-predicted"] = compd_or_predctd
        info["name"] = f"{filepath.parts[-2]}_{filepath.stem}_{i}"
        config.info = {k: v for k, v in info.items() if v is not None}
        configs.append(config)
    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "isotropic-polarizability": {"field": "isotropic-polarizability"},
        "rmsd": {"field": "rmsd"},
        "computed-or-predicted": {"field": "computed-or-predicted"},
        "target-tanimoto-similarity": {"field": "target-tanimoto-similarity"},
        "homo-lumo-gap": {"field": "gap"},
        "relative-atomic-energy": {"field": "relative-atomic-energy"},
        "changed": {"field": "changed"},
        "equals": {"field": "equals"},
        "computed_energy-U0-uncompensated": {
            "field": "computed_energy_U0_uncompensated"
        },
        "gen-idx": {"field": "gen_idx"},
        "known": {"field": "known"},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "0_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ]
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
        [
            f"{DATASET}-polarizability-predicted",
            "1.*generated.*",
            f"Configurations from {DATASET} dataset with properties predicted using cG-SchNet model trained on isotropic polarizability data",
        ],
        [
            f"{DATASET}-polarizability-computed",
            "1.*relaxed.*",
            f"Configurations from {DATASET} dataset with relaxation properties computed using ORCA, based on cG-Schnet model trained on isotropic polarizability data",
        ],
        [
            f"{DATASET}-fingerprint-predicted",
            "2.*generated.*",
            f"Configurations from {DATASET} dataset with properties predicted using cG-SchNet model trained on vector-valued molecular fingerprints",
        ],
        [
            f"{DATASET}-fingerprint-computed",
            "2.*relaxed.*",
            f"Configurations from {DATASET} dataset with relaxation properties computed using ORCA, based on cG-Schnet model trained on vector-valued molecular fingerprints",
        ],
        [
            f"{DATASET}-gap-predicted",
            "3.*generated.*",
            f"Configurations from {DATASET} dataset with properties predicted using cG-SchNet model trained on HOMO-LUMO gap data",
        ],
        [
            f"{DATASET}-gap-computed",
            "3.*relaxed.*",
            f"Configurations from {DATASET} dataset with relaxation properties computed using ORCA, based on cG-Schnet model trained on HOMO-LUMO gap data",
        ],
        [
            f"{DATASET}-composition-relative-energy-predicted",
            "4.*generated.*",
            f"Configurations from {DATASET} dataset with properties predicted using cG-SchNet model trained on atomic composition and relative atomic energy data",
        ],
        [
            f"{DATASET}-composition-relative-energy-computed",
            "4.*relaxed.*",
            f"Configurations from {DATASET} dataset with relaxation properties computed using ORCA, based on cG-Schnet model trained on atomic composition and relative atomic energy data",
        ],
        [
            f"{DATASET}-gap-relative-energy-predicted",
            "5.*generated.*",
            f"Configurations from {DATASET} dataset with properties predicted using cG-SchNet model trained on HOMO-LUMO gap and relative atomic energy data",
        ],
        [
            f"{DATASET}-gap-computed",
            "5.*relaxed.*",
            f"Configurations from {DATASET} dataset with relaxation properties computed using ORCA, based on cG-Schnet model trained on HOMO-LUMO gap and relative atomic energy data",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={
                "hash": {"$in": all_co_ids},
                "names": {"$regex": regex},
            },
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
        else:
            pass

    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys


from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from ase.io.vasp import read_vasp

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets"
    "/Ti:Mo_alloys_SilvaAndrea/"
)
DATASET = "TiMoS_alloys_CMS2021"

PUBLICATION = "https://doi.org/10.1016/j.commatsci.2020.110044"
DATA_LINK = "https://eprints.soton.ac.uk/443461/"
LINKS = [
    "https://doi.org/10.1016/j.commatsci.2020.110044",
    "https://eprints.soton.ac.uk/443461/",
]
AUTHORS = ["Andrea Silva", "Tomas Polcar", "Denis Kramer"]
DS_DESC = (
    "Training set (DFT output) for CE models and MC simulation "
    "output for the manuscript 'Phase behaviour of (Ti:Mo)S2binary "
    "alloys arising from electron-lattice coupling'. The DFT "
    "calculations are performed using VASP 5.4.3, compiled with intel "
    "MPI and Intel MKL support."
)

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "VASP 5.4.3"},
                "method": {"value": "DFT-SCAN+rVV10"},
                "kpoints": {"value": "11x11x11"},
                "encut": {"value": "800 eV"},
                "ediff": {"value": 0.0005},
            },
        }
    ],
}


# TiMo
def reader_TiMo(p):
    s = str(p).split("/")
    atom = read_vasp(p)
    e_f = str(p).replace("CONTCAR", "energy")
    with open(e_f, "r") as f:
        energy = float(f.readline())
        atom.info["energy"] = energy
        atom.info["_name"] = "%s_%s" % (s[-3], s[-2])
        return [atom]


def tform(c):
    c.info["per-atom"] = False


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
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="_name",
        elements=["Ti", "Mo", "S"],
        default_name="TiMo",
        reader=reader_TiMo,
        glob_string="*/*/CONTCAR",
        verbose=True,
        generator=False,
    )
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    cs_regexes = {
        "CE_ML_1H": "Configurations of machine learning 1H ",
        "CE_ML_1T": "Configurations of machine learning 1T",
        "CE_bulk_1T": "Configurations of bulk state 1T phase octahedral symmetry",
        "CE_bulk_2H": "Configurations of bulk state 2H phase trigonal prismatic "
        "coordination",
    }

    cs_names = ["CE_ML_1H", "CE_ML_1T", "CE_bulk_1T", "CE_bulk_2H"]

    cs_ids = []

    for i, (regex, desc) in enumerate(cs_regexes.items()):
        co_ids = client.get_data(
            "configurations",
            fields="hash",
            query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
            ravel=True,
        ).tolist()

        print(
            f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7)
        )

        cs_id = client.insert_configuration_set(
            co_ids, description=desc, ds_id=ds_id, name=cs_names[i]
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DATASET,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

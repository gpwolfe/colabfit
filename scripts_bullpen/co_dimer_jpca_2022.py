"""
author: Gregory Wolfe, Alexander Tao

Properties
----------
potential energy
atomic forces

File notes
----------
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read
import pandas as pd

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import potential_energy_pd

DS_NAME = "Co_dimer_JPCA_2022"
# Mongo pod
# DS_PATH = Path(
#     "/persistent/colabfit_raw_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/"
# )
DS_PATH = Path().cwd().parent / "data/Co_dimer_data"  # Local
XYZ_PATH = DS_PATH / "structures_xyz"

PUBLICATION = "https://doi.org/10.1021/acs.jpca.1c08950"
DATA_LINK = "https://doi.org/10.24435/materialscloud:pe-zv"
LINKS = [
    "https://doi.org/10.1021/acs.jpca.1c08950",
    "https://doi.org/10.24435/materialscloud:pe-zv",
]
AUTHORS = [
    "Sijin Ren",
    "Eric Fonseca",
    "William Perry",
    "Hai-Ping Cheng",
    "Xiao-Guang Zhang",
    "Richard Hennig",
]
DS_DESC = (
    "This dataset contains dimer molecules of Co(II) with potential energy "
    "calculations for structures with ferromagnetic and antiferromagnetic "
    "spin configurations. Calculations were carried out in Gaussian 16 with "
    "the PBE exchange-correlation functional and 6-31+G* basis set. "
    "All molecules contain the same atomic core region, consisting of "
    "the tetrahedral and octahedral Co centers and the three PO2R2 bridging "
    "ligands. The ligand exchange provides a broad range of exchange energies "
    "(ΔEJ), from +50 to -200 meV, with 80% of the ligands yielding ΔEJ < 10 meV."
)


property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "a.u."},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": {
                "software": {"value": "Gaussian 16"},
                "method": {"value": "DFT-PBE"},
                "basis-set": {"value": "6-31+G*"},
            },
        }
    ],
}


def reader(fp):
    df = pd.read_csv(fp, index_col=0, header=0)
    structures_FM = []
    structures_AFM = []
    for i, row in enumerate(df.index):
        file_FM = XYZ_PATH / df.loc[row, "xyz_filename_FM"]
        structure_FM = read(file_FM)
        structure_FM.info["energy"] = df.loc[row, "E-FM(a.u.)"].item()
        structure_FM.info["name"] = file_FM.stem
        structures_FM.append(structure_FM)

        file_AFM = XYZ_PATH / df.loc[row, "xyz_filename_AFM"]
        structure_AFM = read(file_AFM)
        structure_AFM.info["energy"] = df.loc[row, "E-AFM(a.u.)"].item()
        structure_AFM.info["name"] = file_AFM.stem
        structures_AFM.append(structure_AFM)

    return structures_FM + structures_AFM


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

    client.insert_property_definition(potential_energy_pd)

    dss = (
        (
            "Co_dimer_JPCA_2022_train",
            "Co_dimer_data.csv",
            "Training data only from the Co_dimer_JPCA_2022 dataset. " + DS_DESC,
        ),
        (
            "Co_dimer_JPCA_2022",
            "Co_dimer_data_all.csv",
            DS_DESC,
        ),
    )
    for name, glob, desc in dss:
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=DS_PATH,
            file_format="folder",
            name_field="name",
            elements=None,
            reader=reader,
            glob_string=glob,
            verbose=True,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_pr_ids = list(zip(*ids))

        css = (
            (
                "co_dimer_ferromagnetic",
                "_FM",
                "Structures of Co(II) with ferromagnetic spin configurations",
            ),
            (
                "co_dimer_antiferromagnetic",
                "_AFM",
                "Structures of Co(II) with antiferromagnetic spin configurations",
            ),
        )
        for csname, csreg, csdesc in css:
            client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                name=csname,
                ds_id=ds_id,
                description=csdesc,
                query={"names": {"$regex": csreg}},
            )

        client.insert_dataset(
            do_hashes=all_pr_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=LINKS,
            description=desc,
            resync=True,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

#!/usr/bin/env python
# coding: utf-8
"""
File notes:
----------
There my be reason to go back and add a configuration set of the
training data, since this appears to be a subset of the total data
but obtainable from a separate file. --gpw
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import iread
import numpy as np

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id

from colabfit.tools.property_definitions import (
    potential_energy_pd,
    cauchy_stress_pd,
    atomic_forces_pd,
)

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/colabfit_data/new_raw_datasets"
    "/Carbon_GAP_20/Carbon_GAP_20/"
)
DATASET_FP = Path().cwd().parent / "data/Carbon_GAP_20"
DATASET = "Carbon_GAP_JCP2020"
DATA_LINK = "https://www.repository.cam.ac.uk/handle/1810/307452"
PUBLICATION = "https://doi.org/10.1063/5.0005084"
LINKS = [
    "https://doi.org/10.1063/5.0005084",
    "https://www.repository.cam.ac.uk/handle/1810/307452",
]
AUTHORS = [
    "Patrick Rowe",
    "Volker L. Deringer",
    "Piero Gasparotto",
    "Gábor Csányi",
    "Angelos Michaelides",
]
DS_DESC = (
    "GAP-20 describes the properties "
    "of the bulk crystalline and amorphous phases, crystal surfaces, and defect "
    "structures with an accuracy approaching that of direct ab initio simulation, "
    "but at a significantly reduced cost. The final potential is fitted to reference "
    "data computed using the optB88-vdW density functional theory (DFT) functional."
)

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"value": "DFT/optB88-vdW"},
    "input": {"field": "input"},
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "force", "units": "eV/Ang"},
            "_metadata": PI_MD,
        }
    ],
    "cauchy-stress": [
        {
            "stress": {"field": "virial", "units": "eV"},
            "volume-normalized": {"value": True, "units": None},
            "_metadata": PI_MD,
        }
    ],
}
DSS = (
    ("Carbon_GAP_JCP_2020", "Carbon_Data_Set_Total.xyz", DS_DESC),
    (
        "Carbon_GAP_JCP_2020_train",
        "Carbon_GAP_20_Training_Set.xyz",
        "Training data generated for GAP-20. " + DS_DESC,
    ),
)
CSS = [
    (
        "_Liquid_Interface",
        "Liquid_Interface",
        "Liquid interface configurations from Carbon_GAP_20",
    ),
    (
        "_Crystalline_Bulk",
        "Crystalline_Bulk",
        "Crystalline bulk configurations from Carbon_GAP_20",
    ),
    ("_Graphene", "Graphene", "Graphene configurations from Carbon_GAP_20"),
    ("_LD_Iter1", "LD_Iter1", "LD_Iter1 configurations from Carbon_GAP_20"),
    ("_SACADA", "SACADA", "SACADA configurations from Carbon_GAP_20"),
    (
        "_Graphite_Layer_Sep",
        "Graphite_Layer_Sep",
        "Graphite layer sep configurations from Carbon_GAP_20",
    ),
    ("_Dimer", "Dimer", "Dimer configurations from Carbon_GAP_20"),
    ("_Liquid", "Liquid", "Liquid configurations from Carbon_GAP_20"),
    (
        "_Amorphous_Surfaces",
        "Amorphous_Surfaces",
        "Amorphous surfaces configurations from Carbon_GAP_20",
    ),
    (
        "_Amorphous_Bulk",
        "Amorphous_Bulk",
        "Amorphous bulk configurations from Carbon_GAP_20",
    ),
    ("_Surfaces", "Surfaces", "Surfaces configurations from Carbon_GAP_20"),
    (
        "_Single_Atom",
        "Single_Atom",
        "Single atom configurations from Carbon_GAP_20",
    ),
    (
        "_Nanotubes",
        "Nanotubes",
        "Nanotube configurations from Carbon_GAP_20",
    ),
    (
        "_Fullerenes",
        "Fullerenes",
        "Fullerene configurations from Carbon_GAP_20",
    ),
    ("_Defects", "Defects", "Defect configurations from Carbon_GAP_20"),
    ("_Diamond", "Diamond", "Diamond configurations from Carbon_GAP_20"),
    ("_Graphite", "Graphite", "Graphite configurations from Carbon_GAP_20"),
    (
        "_Crystalline_RSS",
        "Crystalline_RSS",
        "Crystalline RSS configurations from Carbon_GAP_20",
    ),
]
CO_MD = {key: {"field": key} for key in ["cutoff", "nneightol"]}


def reader(fp):
    atoms = []
    for atom in iread(fp, index=":"):
        input = {"encut": {"value": 600, "units": "eV"}}
        kpoints = atom.info.get("kpoints")
        if kpoints is not None:
            if isinstance(kpoints, np.ndarray):
                kpoints = kpoints.tolist()
            input["kpoints"] = kpoints
        k_density = atom.info.get("kpoints_density")
        if k_density is not None:
            if isinstance(k_density, np.ndarray):
                k_density = k_density.tolist()
            input["kpoints-density"] = k_density
        atom.info["input"] = input
        atoms.append(atom)
    return atoms


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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(cauchy_stress_pd)

    for ds_name, ds_glob, ds_desc in DSS:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="config_type",
            elements=["C"],
            default_name=None,
            verbose=True,
            reader=reader,
            glob_string=ds_glob,
            generator=True,
        )
        ds_id = generate_ds_id()
        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                co_md_map=CO_MD,
                property_map=PROPERTY_MAP,
                generator=False,
                transform=tform,
                verbose=True,
            )
        )

        all_co_ids, all_pr_ids = list(zip(*ids))

        cs_ids = []
        for i, (cs_name, cs_regex, desc) in enumerate(CSS):
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=ds_name + cs_name,
                description=desc,
                query={"names": {"$regex": cs_regex}},
            )

        cs_ids.append(cs_id)

        client.insert_dataset(
            do_hashes=all_pr_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=ds_desc,
            resync=True,
            verbose=True,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

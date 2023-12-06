"""
author: Gregory Wolfe

Properties
----------
energy
forces

Other properties added to metadata
----------------------------------

File notes
----------
there are five files with missing/corrupted contents. on 1 Nov. 2023 raised GH issue
in the meantime, I have just altered the file names to prevent glob recognition
    sti_train_s2.xyz
    sti_val_s0.xyz
    sti_val_s1.xyz
    sti_val_s2.xyz (all from CASSCF set)
    sti_train_uks.xyz (from DFT set)

additionally, xxMD-main/xxMD-CASSCF/sti/s1/sti_s1/sti_train_s1.xyz is corrupted at the
end. It is not clear how many configurations are cut off, but I have truncated the
file and used what is there.

For our xxMD dataset, we diverge from the MD17’s framework by employing
the trajectory surface hopping dynamics algorithm in tandem with the
state-averaged complete active state self-consistent field (SA-CASSCF)
electronic theory. This approach stands in contrast to the adiabatic
dynamics used in MD17. The reason for the pivot to SA-CASSCF lies in its adeptness
at handling electronic correlation effects at strongly deformed geometries, where
KS-DFT often falls short, especially in scenarios involving multiple electronic
states and conical intersections.
Nevertheless, to ensure compatibility with prevalent datasets like MD17, we also
computed single- point spin-polarized KS-DFT (or unrestricted KS-DFT) values.
These calculations leverage the M06[26] exchange-correlation functional—a notably
superior meta-GGA functional relative to PBE. This dual approach culminates in two
datasets: xxMD-CASSCF and xxMD-DFT. The former captures potential energies and forces
across the first three electronic states for azobenzene, dithiophene, malonaldehyde,
and stilbene. The latter provides recomputed ground-state energy and force values,
anchored to the same trajectories. Both xxMD datasets are structured via a temporal
split method, partitioning training and testing data based on trajectory timesteps.

                Method
Dithiophene     SA-CASSCF(10e,10o)/6-31g
Azobenzene      SA-CASSCF(6e,6o)/6-31g
Malonaldehyde   SA-CASSCF(8e,6o)/6-31g
Stilbene        SA-CASSCF(2e,2o)/6-31g*
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

from ase.io import read

# from pymongo.errors import InvalidOperation

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    # cauchy_stress_pd,
    potential_energy_pd,
)


DATASET_FP = Path("data/xxmd")

PUBLICATION = "https://doi.org/10.48550/arXiv.2308.11155"
DATA_LINK = "https://github.com/zpengmei/xxMD"
LINKS = [
    "https://github.com/zpengmei/xxMD",
    "https://doi.org/10.48550/arXiv.2308.11155",
]
AUTHORS = ["Zihan Pengmei", "Yinan Shu", "Junyu Liu"]
DATASET_DESC = (
    "The xxMD (Extended Excited-state Molecular Dynamics) dataset is a comprehensive "
    "collection of non-adiabatic trajectories encompassing several photo-sensitive "
    "molecules. This dataset challenges existing Neural Force Field (NFF) models "
    "with broader nuclear configuration spaces that span reactant, transition state, "
    "product, and conical intersection regions, making it more chemically "
    "representative than its contemporaries. xxMD is divided into two datasets, each "
    "with corresponding train, test and validation splits. xxMD-CASSCF contains "
    "calculations generated using state-averaged complete active state self-consistent "
    "field (SA-CASSCF) electronic theory. xxMD-DFT contains recalculated single-point "
    "spin-polarized (unrestricted) DFT values."
)
ELEMENTS = None
GLOB_STR = "*.*"

PI_METADATA_CASS = {
    "software": {"value": "OpenMolcas 22.06"},
    "method": {"value": "SA-CASSCF"},
}
PI_METADATA_DFT = {
    "software": {"value": "Psi4"},
    "method": {"value": "DFT-M06"},
    "basis-set": {"value": "6-31g"},
}

PROPERTY_MAP_CASS = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA_CASS,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA_CASS,
        },
    ],
}
PROPERTY_MAP_DFT = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA_DFT,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_METADATA_DFT,
        },
    ],
}

DSS = (
    # xxMD CASSCF
    (
        "xxMD-CASSCF_test",
        DATASET_FP / "xxMD-CASSCF",
        "*test*",
        PROPERTY_MAP_CASS,
        "Test dataset from xxMD-CASSCF. " + DATASET_DESC,
    ),
    (
        "xxMD-CASSCF_train",
        DATASET_FP / "xxMD-CASSCF",
        "*train*",
        PROPERTY_MAP_CASS,
        "Training dataset from xxMD-CASSCF. " + DATASET_DESC,
    ),
    (
        "xxMD-CASSCF_validation",
        DATASET_FP / "xxMD-CASSCF",
        "*val*",
        PROPERTY_MAP_CASS,
        "Validation dataset from xxMD-CASSCF. " + DATASET_DESC,
    ),
    # xxMD DFT
    (
        "xxMD-DFT_test",
        DATASET_FP / "xxMD-DFT",
        "*test*",
        PROPERTY_MAP_DFT,
        "Test dataset from xxMD-DFT. " + DATASET_DESC,
    ),
    (
        "xxMD-DFT_train",
        DATASET_FP / "xxMD-DFT",
        "*train*",
        PROPERTY_MAP_DFT,
        "Training dataset from xxMD-DFT. " + DATASET_DESC,
    ),
    (
        "xxMD-DFT_validation",
        DATASET_FP / "xxMD-DFT",
        "*val*",
        PROPERTY_MAP_DFT,
        "Validation dataset from xxMD-DFT. " + DATASET_DESC,
    ),
)

MOL_DICT = {
    "azo": "azobenzene",
    "mal": "malonaldehyde",
    "dia": "dithiophene",
    "sti": "stilbene",
}


def namer(fp):
    mol, split, batch = fp.stem.split("_")
    molecule = MOL_DICT[mol]
    if "xxMD-CASSCF" in str(fp):
        batch = batch.replace("s", "state")
        name = f"xxMD-CASSCF_{molecule}_{split}_{batch}"
    else:
        name = f"xxMD-DFT_{molecule}_{split}"
    return name, molecule, split


def reader(fp):
    with open("files_read.txt", "a") as f:
        f.write(f"{fp}\n")
    name, mol, split = namer(fp)
    print(fp)
    configs = read(fp, index=":")
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
        config.info["labels"] = [mol]
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
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    for ds_name, dir_path, glob, property_map, ds_desc in DSS:
        css = [
            [
                f"{ds_name}_azobenzene",
                {"names": {"$regex": "azobenzene"}},
                f"Configurations of azobenzene from {ds_name} dataset",
            ],
            [
                f"{ds_name}_malonaldehyde",
                {"names": {"$regex": "malonaldehyde"}},
                f"Configurations of malonaldehyde from {ds_name} dataset",
            ],
            [
                f"{ds_name}_dithiophene",
                {"names": {"$regex": "dithiophene"}},
                f"Configurations of dithiophene from {ds_name} dataset",
            ],
            [
                f"{ds_name}_stilbene",
                {"names": {"$regex": "stilbene"}},
                f"Configurations of stilbene from {ds_name} dataset",
            ],
        ]
        ds_id = generate_ds_id()

        configurations = load_data(
            file_path=dir_path,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=glob,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations=configurations,
                ds_id=ds_id,
                # co_md_map=CO_METADATA,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        cs_ids = []
        for i, (cs_name, query, cs_desc) in enumerate(css):
            # try:
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                ds_id=ds_id,
                name=cs_name,
                description=cs_desc,
                query=query,
            )
            cs_ids.append(cs_id)
            # except InvalidOperation:
            #     pass

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=ds_desc,
            verbose=True,
            cs_ids=cs_ids,  # remove line if no configuration sets to insert
        )


if __name__ == "__main__":
    main(sys.argv[1:])

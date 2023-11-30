"""
author:gpwolfe

Data can be downloaded from:
https://zenodo.org/record/7112198#.Y-pqhxOZPMI
File address:
https://zenodo.org/record/7112198/files/mbgdml-h2o-meoh-mecn-md.zip?download=1

Unzip files to data directory:
unzip mbgdml-h2o-meoh-mecn-md.zip "*.npz" -d <project_directory>/script/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties:
potential energy
forces

Other properties added to metadata:
velocities
kinetic energy

File notes
----------
Where methods=MBE, scripts used mbGDML many-body expansion predictor

npz file keys:
md_data = {
    'z': ase_atoms, element numbers
    'R': R,  positions
    'E_potential': E_potential,
    'E_kinetic': E_kinetic,
    'F': F, forces in kcal/mol/Angstrom
    'V': Vel, velocity
    'type': 'md',
    'e_unit': 'kcal/mol',
    'r_unit': 'Angstrom',
    'v_unit': 'Angstrom/fs',
    'entity_ids': entity_ids,
    'comp_ids': comp_ids,
    'name': md_name
}

.npz files contain same plus additional data as .xyz files
save_path = working_dir + md_name + '.npz'
np.savez_compressed(save_path, **md_data)
write_xyz(md_name+'.xyz', ase_atoms, R, working_dir)

For ORCA:
method: 'MP2 def2-TZVP'

For gfn2
method: "GFN2-xTB"

For gdml
method: MBE

From schnet:
method: MBE

from gap
method: MBE
"""
from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from collections import defaultdict, namedtuple
import numpy as np
from pathlib import Path
import sys

DATASET_FP = Path(
    "/persistent/colabfit_raw_data/gw_scripts/gw_script_data/mbgdml"
    "/mbgdml-h2o-meoh-mecn-md/"
)

DS_NAME = "mbGDML_maldonado_2023"

AUTHORS = ["Alex M. Maldonado"]
PUBLICATION = "https://doi.org/10.26434/chemrxiv-2023-wdd1r"
DATA_LINK = "https://doi.org/10.5281/zenodo.7112197"
LINKS = [
    "https://doi.org/10.5281/zenodo.7112197",
    "https://doi.org/10.26434/chemrxiv-2023-wdd1r",
]
DS_DESC = (
    "Configurations of water, acetonitrile and methanol,"
    " simulated with ASE and modeled using a variety of software and"
    " methods: GAP, SchNet, GDML, ORCA and mbGDML. Forces and potential"
    " energy included; metadata includes kinetic energy and velocities."
)
soft_meth = namedtuple("soft_meth", ["method", "software"])
# The [-3]rd element of the Path(filepath).parts == key below
method_soft_dict = {
    "gfn2": soft_meth("GFN2-xTB", "ORCA"),
    "orca": soft_meth("MP2", "ORCA"),
    "schnet": soft_meth("SchNet", "ORCA"),
    "gap": soft_meth("GAP", "ORCA"),
    "gdml": soft_meth("mbGDML", "ORCA"),
}


def read_npz(filepath):
    data = defaultdict(list)
    with np.load(filepath, allow_pickle=True) as f:
        for key in f.files:
            data[key] = f[key]
    return data


def reader(filepath):
    filepath = Path(filepath)
    method, software = method_soft_dict[filepath.parts[-3]]

    data = read_npz(filepath)

    atoms = [
        AtomicConfiguration(positions=data["R"][i], numbers=data["z"].tolist())
        for i, val in enumerate(data["R"])
    ]

    array_keys = ("V", "F", "E_kinetic", "E_potential")
    non_array_keys = ("e_unit", "r_unit", "v_unit", "name")
    for i, atom in enumerate(atoms):
        for key in array_keys:
            atom.info[key] = data[key][i].tolist()
        for key in non_array_keys:
            atom.info[key] = data[key].tolist()
        atom.info["software"] = software
        atom.info["method"] = method

    return atoms


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

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "O", "C", "N"],
        reader=reader,
        glob_string="*.npz",
        generator=False,
    )
    pds = [atomic_forces_pd, potential_energy_pd]
    for pd in pds:
        client.insert_property_definition(pd)
    metadata = {
        "software": {"field": "software"},
        "method": {"field": "method"},
    }
    co_md_map = {
        "velocities": {"field": "V"},
        "velocity-units": {"field": "v_unit"},
        "kinetic_energy": {"field": "E_kinetic"},
        "kinetic_units": {"field": "e_unit"},
    }

    property_map = {
        "potential-energy": [
            {
                "energy": {
                    "field": "E_potential",
                    "units": {"field": "e_unit"},
                },
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "F", "units": "kcal/mol Angstrom"},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            co_md_map=co_md_map,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    cs_regexes = [
        [
            "mbGDML ",
            "md-mbgdml",
            "Configurations from the mbGDML set predicted using mbgDML",
        ],
        [
            "GAP",
            "md-gap",
            "Configurations from the mbGDML set predicted using GAP",
        ],
        [
            "SchNet",
            "md-schnet",
            "Configurations from the mbGDML set predicted using SchNet",
        ],
        [
            "GFN2",
            "md-gfn2",
            "Configurations from the mbGDML set predicted using XTB at GFN2 "
            "level of theory",
        ],
        [
            "ORCA",
            "md-orca",
            "Configurations from the mbGDML set predicted using ORCA",
        ],
    ]

    cs_ids = []
    for i, (name, regex, desc) in enumerate(cs_regexes):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query={"names": {"$regex": regex}},
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids=cs_ids,
        ds_id=ds_id,
        do_hashes=all_do_ids,
        name=DS_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

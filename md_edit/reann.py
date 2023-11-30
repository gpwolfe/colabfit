"""
author:gpwolfe

Data can be downloaded from:
https://github.com/zhangylch/REANN
Download link:
https://github.com/zhangylch/REANN/archive/refs/heads/main.zip

Unzip to project folder
unzip REANN-main.zip "*/configuration" -d $project_dir/scripts/reann

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------

File notes
----------
CO2+Ni(100) reactive system


"""
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/reann")
DATASET_FP = Path().cwd().parent / "data/reann"
DATASET = "REANN_CO2_Ni100"

SOFTWARE = "VASP"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1021/acs.jpclett.9b00085"
DATA_LINK = "https://github.com/zhangylch/REANN"
LINKS = [
    "https://github.com/zhangylch/REANN",
    "https://doi.org/10.1021/acs.jpclett.9b00085",
    "https://doi.org/10.1103/PhysRevLett.127.156002",
    "https://doi.org/10.1063/5.0080766",
]
AUTHORS = ["Yaolong Zhang", "Junfan Xia", "Bin Jiang"]
DS_DESC = "Approximately 9,850 configurations of CO2 with a movable Ni(100) surface."


def reader(filepath):
    coor = []
    scalmatrix = []
    abprop = []
    force = None
    atom = []
    mass = []
    numatoms = []
    period_table = []
    force = []
    numpoint = 0
    num = 0
    nprob = 1
    with open(filepath, "r") as f1:
        while True:
            string = f1.readline()
            if not string:
                break
            string = f1.readline()
            scalmatrix.append([])
            m = list(map(float, string.split()))
            scalmatrix[num].append(m)
            string = f1.readline()
            m = list(map(float, string.split()))
            scalmatrix[num].append(m)
            string = f1.readline()
            m = list(map(float, string.split()))
            scalmatrix[num].append(m)
            string = f1.readline()
            m = list(map(float, string.split()[1:4]))
            period_table.append(m)
            coor.append([])
            mass.append([])
            atom.append([])
            # if start_table==1:
            force.append([])
            while True:
                string = f1.readline()
                m = string.split()
                if m[0] == "abprop:":
                    abprop.append(list(map(float, m[1 : 1 + nprob])))
                    break
                else:
                    atom[num].append(m[0])
                    tmp = list(map(float, m[1:]))
                    mass[num].append(tmp[0])
                    coor[num].append(tmp[1:4])
                    force[num].append(tmp[4:7])
            numpoint += 1
            numatoms.append(len(atom[num]))
            num += 1
    configs = []
    for i, coord in enumerate(coor):
        config = AtomicConfiguration(
            positions=coord,
            symbols=atom[i],
            cell=scalmatrix[i],
            pbc=period_table[i],
        )
        config.info["name"] = f"co2_Ni100_{filepath.parts[-2]}_{i}"
        config.info["force"] = force[i]
        config.info["energy"] = abprop[i][0]
        configs.append(config)

    return configs


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
        elements=["O", "C", "Ni"],
        reader=reader,
        glob_string="configuration",
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "encut": {"value": "400 eV"},
        "kpoints": {"value": "3 x 3 x 1"},
        "ismear": {"value": -1},
        "sigma": {"value": "0.1 eV"}
        # "": {"field": "", "units": ""}
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "force", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }
    ds_id = generate_ds_id()
    ids = list(
        client.insert_data(
            configurations,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_do_ids,
        name=DATASET,
        ds_id=ds_id,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

"""
author: Gregory Wolfe
This script runs JARVIS ingest scripts that do not have problems on ingest
as of 6 Sept. 2023.
Most of the problems relate to a dataset having too many band-gap energy values
equal to 0.0, which runs the property instance above the BSON size limit.
Those scripts with large numbers of 0.0-value band-gap are:
cfid_3d_2021:   37,589

megnet:         24,383
qe_tb:          750,134
dft3_2021       37,589
dft3_2022       52,932
mp_all          over 20,000

Other datasets cause the dataset object to be too large.
ocp_all
oqmd_no_cfid



"""

from argparse import ArgumentParser
from datetime import datetime
import os
from pathlib import Path
import sys

j_scripts = sorted(list(Path().cwd().glob("*.py")))


def main(db_ip, db_name, nprocs, port):
    for script in j_scripts:
        if script.stem not in [
            "__init__",
            "colabfit_utilities",
            "jarvis_agra_cho",
            "jarvis_agra_co",
            "jarvis_agra_cooh",
            "jarvis_agra_o",
            "jarvis_agra_oh",
            "jarvis_alignn_ff",
            "jarvis_c2db",
            "jarvis_epc_data_2d",
            "jarvis_hopv",
            "jarvis_mlearn",
            "jarvis_mp_84",
            "jarvis_ocp_100K",
            "jarvis_ocp_10k",
            "jarvis_omdb",
            "jarvis_polymer_genome",
            "run_jarvis_without_problem_scripts",
        ]:
            print(script)
            exit_code = os.system(
                f"python {script} -i {db_ip} -d {db_name} -p {nprocs} -r {port}"
            )
            if exit_code != 0:
                print(f"Error running {script.name}")
                with open("jarvis_script_errors.log", "a") as f:
                    f.write(
                        f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}    {script}\n"
                    )
            else:
                with open("jarvis_ingested.txt", "a") as fg:
                    fg.write(
                        f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}    {script}\n"
                    )


if __name__ == "__main__":
    argvs = sys.argv[1:]
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
    args = parser.parse_args(argvs)

    main(args.ip, args.db_name, args.nprocs, args.port)

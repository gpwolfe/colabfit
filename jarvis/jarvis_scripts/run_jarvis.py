from argparse import ArgumentParser
from datetime import datetime
import os
from pathlib import Path
import sys

j_scripts = Path().cwd().glob("*.py")


def main(db_ip, db_name, nprocs):
    for script in j_scripts:
        if script.stem not in [
            "run_jarvis",
            "jarvis_ocp_all",
            "jarvis_ocp_100K",
            "jarvis_mp_all",
        ]:
            print(script)
            exit_code = os.system(
                f"python {script} -i {db_ip} -d {db_name} -p {nprocs}"
            )
            if exit_code != 0:
                print(f"Error running {script.name}")
                with open("jarvis_script_errors.log", "a") as f:
                    f.write(
                        f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}    {script}\n"
                    )


if __name__ == "__main__":
    argvs = sys.argv[1:]
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--ip",
        type=str,
        help="IP of host mongod",
    )
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
    )
    args = parser.parse_args(argvs)
    main(args.ip, args.db_name, args.nprocs)

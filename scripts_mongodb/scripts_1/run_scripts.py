from argparse import ArgumentParser
from datetime import datetime
import os
from pathlib import Path
import sys


def main(db_ip, db_name, nprocs, port):
    cwd = Path.cwd()
    for script in sorted(list(cwd.glob("*.py"))):
        if script.stem == "run_scripts":
            pass
        else:
            print(script)
            exit_code = os.system(
                f"python {script} -i {db_ip} -d {db_name} -p {nprocs} -r {port}"
            )
            if exit_code != 0:
                print(f"Error running {script.name}")
                with open(cwd / "script_run_errors.txt", "a") as f:
                    f.write(
                        f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}    {script} \n"
                    )


if __name__ == "__main__":
    args = sys.argv[1:]
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--ip",
        type=str,
        help="IP of host mongod",
        default="localhost"
        # default="10.32.250.13",
    )
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add datasets to",
        default="cf-update-2023-11-30",
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
    args = parser.parse_args(args)
    main(args.ip, args.db_name, args.nprocs, args.port)
# python run_scripts.py -i 10.32.250.13 -d cf-update-2023-11-30 -p 16 -r 30007

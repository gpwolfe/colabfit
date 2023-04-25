from argparse import ArgumentParser
from datetime import datetime
import os
from pathlib import Path
import sys


def main(db_ip, db_name, nprocs):
    # To limit scripts ingested, use for-loop line below instead.
    # for script in list((Path().cwd() / "scripts").rglob("*.py"))[10:]:
    cwd = Path.cwd()
    for script in sorted(list((cwd / "edited_gpw").rglob("*.py"))):
        print(script, flush=True)
        os.chdir(script.parent)
        exit_code = os.system(f"python {script} -i {db_ip} -d {db_name} -p {nprocs}")
        if exit_code != 0:
            print(f"Error running {script.name}")
            with open(cwd / "script_run_errors_edited_gw.txt", "a") as f:
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
        default="10.0.104.220",
    )
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add datasets to",
        default="test_db_gw",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(args)
    main(args.ip, args.db_name, args.nprocs)

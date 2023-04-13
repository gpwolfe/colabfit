from argparse import ArgumentParser
from datetime import datetime
import os
from pathlib import Path
import sys


def main(db_ip):
    # To limit scripts ingested, use for-loop line below instead.
    # for script in list((Path().cwd() / "scripts").rglob("*.py"))[10:]:
    cwd = Path.cwd()
    for script in list((cwd / "scripts").rglob("*.py")):
        print(script)
        os.chdir(script.parent)
        exit_code = os.system(f"python {script} -i {db_ip}")
        if exit_code != 0:
            print(f"Error running {script.name}")
            with open(cwd / "script_run_errors.txt", "a") as f:
                f.write(
                    f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}    {script} \n"
                )
    # Once scripts2 has been added
    # cwd = Path.cwd()
    # for script in list((cwd / "scripts").rglob("*.py")):
    #     print(script)
    #     os.chdir(script.parent)
    #     exit_code = os.system(f"python {script} -i localhost")
    #     if exit_code != 0:
    #         print(f"Error running {script.name}")
    #         with open(cwd / "script_run_errors.txt", "a") as f:
    #             f.write(
    #                 f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}    {script} \n"
    #             )


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
    args = parser.parse_args(args)
    main(args.ip)

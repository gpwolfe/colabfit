from tqdm import tqdm
from pathlib import Path
import sys


def main(base_path: str):
    base_fp = Path(base_path)
    if not base_fp.exists():
        print(f"Base path {base_fp} does not exist")
        return

    subdirs = sorted(
        [
            d
            for d in base_fp.iterdir()
            if d.is_dir() and d.name.startswith("array_logs_")
        ],
        key=lambda x: int(x.name.split("_")[2]),
    )
    for subdir in tqdm(subdirs, desc="Processing subdirectories"):
        subdir_num = int(subdir.name.split("_")[2])
        fps = subdir.glob("*.out")
        fps = sorted(list(fps))
        disk_quota = []
        time_limit = []
        other_err = []
        for fp in tqdm(fps, desc=f"Processing files in {subdir.name}", leave=False):
            with open(fp, "r") as f:
                text = f.read()
                if "complete" in text:
                    continue
                elif "Disk quota exceeded" in text:
                    disk_quota.append(subdir_num + int(fp.stem.split("_")[1]))
                elif "DUE TO TIME LIMIT" in text:
                    time_limit.append(subdir_num + int(fp.stem.split("_")[1]))
                elif "complete" in text:
                    continue
                else:
                    other_err.append(subdir_num + int(fp.stem.split("_")[1]))

        with open("disk_quota.txt", "a") as dq_file:
            for num in sorted(disk_quota):
                dq_file.write(f"{num}\n")

        with open("time_limit.txt", "a") as tl_file:
            for num in sorted(time_limit):
                tl_file.write(f"{num}\n")

        with open("other_err.txt", "a") as oe_file:
            for num in sorted(other_err):
                oe_file.write(f"{num}\n")


if __name__ == "__main__":
    main(sys.argv[1])

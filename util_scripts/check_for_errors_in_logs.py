"""
Remember that if you run this script while a job is ingesting, logs will show as
incomplete."""

from pathlib import Path
from pprint import pprint


def get_nums_from_fnames(fnames):
    nums = []
    for fname in fnames:
        num = fname.split(".")[0].split("_")[-1]
        if num:
            nums.append(int(num))
    nums = sorted(nums)
    nums = str(nums)[1:-1].replace(" ", "")
    return nums


def main(file_path: str):
    fp = Path(file_path)
    if not fp.exists():
        print(f"Path {fp} does not exist")
        return
    fps = fp.glob("*.out")
    fps = sorted(list(fps))
    print("Num files:")
    print(len(fps))
    time_limit = []
    disk_quota = []
    duplicates = []
    other_err = []
    for fp in fps:
        with open(fp, "r") as f:
            text = f.read()
            if "complete" in text:
                continue
            elif "Disk quota exceeded" in text:
                disk_quota.append(fp.name)
            elif "DUE TO TIME LIMIT" in text:
                time_limit.append(fp.name)
            elif "ValueError: Duplicate IDs found in table. Not writing" in text:
                duplicates.append(fp.name)
            elif "complete" in text or "Finished!" in text:
                continue
            else:
                other_err.append(fp.name)

    print("disk quota exceeded: ")
    pprint(sorted(disk_quota))
    print("time limit exceeded: ")
    pprint(sorted(time_limit))
    print("duplicates found: ")
    pprint(sorted(duplicates))
    print("error is present, but not like above: ")
    pprint(sorted(other_err))
    print(get_nums_from_fnames(sorted(disk_quota)))
    print(get_nums_from_fnames(sorted(time_limit)))
    print(get_nums_from_fnames(sorted(duplicates)))
    print(get_nums_from_fnames(sorted(other_err)))


if __name__ == "__main__":
    import sys

    main(sys.argv[1])

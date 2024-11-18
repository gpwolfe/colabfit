from pathlib import Path
from argparse import ArgumentParser


def mean_ingest_time(dir):
    dir = Path(dir)
    fps = sorted(list(dir.glob("*.out")))
    ttimes = []
    failed = 0
    succeeded = 0
    for fp in fps:
        isfailed = True
        with open(fp, "r") as f:
            for line in f:
                if "Total time:" in line:
                    ttime = float(line.split("Total time: ")[1])
                    ttimes.append(ttime)
                    succeeded += 1
                    isfailed = False
        if isfailed:
            failed += 1
    print(f"average time: {sum(ttimes) / len(ttimes)}")
    print(f"failed: {failed}")


def main(dir_path):
    mean_ingest_time(dir_path)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("dir_path", type=str)
    args = parser.parse_args()
    main(args.dir_path)

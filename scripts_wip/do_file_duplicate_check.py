"""
author: gpwolfe

Usage
-----
python do_file_duplicate_check.py <dir_path>
"""

from argparse import ArgumentParser
from pathlib import Path


def remove_duplicate_lines_across_files(fdir):

    unique_lines = set()
    fps = sorted(list(fdir.rglob("*.txt")))
    for filepath in fps:
        print(filepath)
        with filepath.open("r") as file:
            lines = file.readlines()
            new_lines = []
            duplicates = []

            for line in lines:
                stripped_line = line.strip()

                if stripped_line not in unique_lines:
                    unique_lines.add(stripped_line)
                    new_lines.append(line)
                else:
                    duplicates.append(line)
        if len(new_lines) > 0:
            print(len(new_lines))
            new_dir = fdir.absolute().parent / (
                filepath.parts[-2] + "_checked_for_duplicates"
            )
            new_dir.mkdir(exist_ok=True)
            new_filepath = new_dir / (filepath.stem + "_checked_for_duplicates.txt")
            with new_filepath.open("w") as new_file:
                new_file.writelines(new_lines)
        if len(duplicates) > 0:
            dup_dir = fdir.absolute().parent / (filepath.parts[-2] + "_duplicates")

            dup_dir.mkdir(exist_ok=True)
            with (dup_dir / (filepath.stem + "_duplicates.txt")).open(
                "w"
            ) as duplicates_file:
                duplicates_file.writelines(duplicates)


# dir_path = Path("DS_wxf8f3t3abul_0_do_ids/DS_wxf8f3t3abul_0_do_ids_original_files")
# dir_path = Path("DS_wxf8f3t3abul_0_co_ids/DS_wxf8f3t3abul_0_co_ids_original_files")
# remove_duplicate_lines_across_files(dir_path)
if __name__ == "__main__":
    # dir_path = Path(
    #     "DS_wxf8f3t3abul_0_do_ids/DS_wxf8f3t3abul_0_do_ids_original_files"
    # )
    # dir_path = Path(
    #     "DS_wxf8f3t3abul_0_co_ids/DS_wxf8f3t3abul_0_co_ids_original_files"
    # )
    parser = ArgumentParser()
    parser.add_argument("dir_path", type=Path)
    args = parser.parse_args()
    print(list(args.dir_path.rglob("*.txt")))
    remove_duplicate_lines_across_files(args.dir_path)

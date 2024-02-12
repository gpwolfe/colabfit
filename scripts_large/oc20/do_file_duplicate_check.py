from pathlib import Path


def remove_duplicate_lines_across_files(dir):

    unique_lines = set()

    for filepath in dir.rglob("*.txt"):
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
            new_dir = filepath.absolute().parents[1] / (
                filepath.parts[-3] + "_checked_for_duplicates"
            )
            new_dir.mkdir(exist_ok=True)
            new_filepath = new_dir / (filepath.stem + "_checked_for_duplicates.txt")
            with new_filepath.open("w") as new_file:
                new_file.writelines(new_lines)
        if len(duplicates) > 0:
            dup_dir = filepath.absolute().parents[1] / (
                filepath.parts[-3] + "_duplicates"
            )
            dup_dir.mkdir(exist_ok=True)
            with (dup_dir / (filepath.stem + "_duplicates.txt")).open(
                "w"
            ) as duplicates_file:
                duplicates_file.writelines(duplicates)


dir_path = Path("DS_dgop3abwq9ya_0_do_ids_copy/DS_dgop3abwq9ya_0_do_ids_original_files")
remove_duplicate_lines_across_files(dir_path)

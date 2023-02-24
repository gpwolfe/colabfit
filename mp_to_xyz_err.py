from ase.io import write
from mp_api.client import MPRester

# import time
import numpy as np
import pickle
from pymatgen.io.ase import AseAtomsAdaptor
import re

with open("tasks_dict.pkl", "rb") as f:
    D = pickle.load(f)

# IX_RE = re.compile(r"Error at (?P<start>\d+) - (?P<end>\d+)")
IX_RE = re.compile(r"Error at (?P<ix>\d+)")


def convert(calc, task_id):
    atoms = []
    for j in range(len(calc)):
        for i in range(len(calc[j]["output"]["ionic_steps"])):
            a = AseAtomsAdaptor.get_atoms(
                calc[j]["output"]["ionic_steps"][i]["structure"]
            )

            a.info["e_fr_energy"] = calc[j]["output"]["ionic_steps"][i][
                "e_fr_energy"
            ]
            a.info["e_wo_entrp"] = calc[j]["output"]["ionic_steps"][i][
                "electronic_steps"
            ]["e_wo_entrp"]
            a.info["e_0_energy"] = calc[j]["output"]["ionic_steps"][i][
                "e_0_energy"
            ]
            a.info["stress"] = np.array(
                calc[j]["output"]["ionic_steps"][i]["stress"]
            )
            a.arrays["forces"] = np.array(
                calc[j]["output"]["ionic_steps"][i]["forces"]
            )
            a.info["material_id"] = D[task_id]["material_id"]
            a.info["task_id"] = task_id
            a.info["name"] = "%s-%s-%s" % (task_id, j, i)
            a.info["calc_type"] = D[task_id]["calc_type"]
            atoms.append(a)
    print("atoms", len(atoms))
    return atoms


# There are about ~360,000 tasks-I found querying for too many
# at one time was very slow so doing 50 bunches of 100 each just
# for testing one could run in parallel over many nodes to speed
# things up


def get_xyz_from_mp(mpr, ind):

    task = list(D.keys())[ind]

    atoms = []
    try:
        doc = mpr.tasks.search(
            task_ids=[task],
            fields=["task_id", "calcs_reversed"],
        )
        doc = doc[0].dict()
        # print(dir(doc))
        # dct = doc.dict()
        atoms.extend(convert(doc["calcs_reversed"], doc["task_id"]))

    #     # print(len(atoms))
    #     write(
    #         f"mp_xyz_files_error_124s/mp_{ind}.xyz",
    #         atoms,
    #     )
    #     return 1
    except KeyError as e:
        print(f"Error at {ind}\n", e)
        with open("error_file_singles_124s.txt", "a") as f:
            f.write(f"Error at {ind}\n")
        return 0


def get_all_xyz_from_mp(mpr, inds):

    tasks = list(D.keys())[inds[0] : inds[1]]

    atoms = []
    # try:
    docs = mpr.tasks.search(
        task_ids=tasks,
        fields=["task_id", "calcs_reversed"],
    )
    for task_doc in docs:

        dct = task_doc.dict()
        atoms.extend(convert(dct["calcs_reversed"], dct["task_id"]))

    print(len(atoms))
    write(
        f"mp_xyz_files_error_redos/mp_{inds[0]}-{inds[1]}.xyz",
        atoms,
    )
    # except:
    #     print(f"Error at {inds[0]} - {inds[1]}")

    #     pass


if __name__ == "__main__":
    with MPRester(api_key="Tv1EAmGQI3XPpPiPcu9HS2C7mtAIlZSY") as mpr:
        # with open("error_file_redo.txt") as err_f:
        with open("error_file_singles.txt") as err_f:
            for line in err_f:
                groups = IX_RE.match(line)
                ix = int(groups.groupdict()["ix"])
                result = get_xyz_from_mp(mpr, ix)
                if result == 0:
                    pass

            # for line in err_f:
            #     groups = IX_RE.match(line)
            #     start = int(groups.groupdict()["start"])
            #     end = int(groups.groupdict()["end"])
            #     get_all_xyz_from_mp(mpr, (start, end))
            #     for ind in range(start, end):
            #         result = get_xyz_from_mp(mpr, ind)
            #         if result == 0:
            #             try:
            #                 get_all_xyz_from_mp(mpr, (ind + 1, end))
            #                 break
            #             except:
            #                 pass

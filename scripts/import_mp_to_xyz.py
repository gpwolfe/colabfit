from ase.io import write, read
from mp_api.client import MPRester
from multiprocessing import Pool
import numpy as np
import pickle
from pymatgen.io.ase import AseAtomsAdaptor


with open("tasks_dict.pkl", "rb") as f:
    D = pickle.load(f)


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
                "e_wo_entrp"
            ]
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
    return atoms


def get_xyz_from_mp(job):
    with MPRester(api_key="PFiAaUsvlnxNzJdOKoI5kWgEmo17rthx") as mpr:
        for k in range(
            job[0], job[1]
        ):  # There are about ~360,000 tasks-I found querying for too many
            # at one time was very slow so doing 50 bunches of 100 each just
            # for testing one could run in parallel over many nodes to speed
            # things up
            atoms = []
            try:
                docs = mpr.tasks.search(
                    task_ids=list(D.keys())[k * 100 : (k + 1) * 100],
                    fields=["task_id", "calcs_reversed"],
                )
            except TypeError as e:
                print(f"{e}")
                pass
            for task_doc in docs:
                dct = task_doc.dict()
                atoms.extend(convert(dct["calcs_reversed"], dct["task_id"]))
            print(len(atoms))
            write(
                "/scratch/work/martiniani/for_gregory/mat_proj_xyz_files/mp_%s.xyz"
                % k,
                atoms,
            )


if __name__ == "__main__":
    inds = [(x, x + 4) for x in range(0, 20, 4)]
    with Pool() as p:
        p.map(get_xyz_from_mp, inds)

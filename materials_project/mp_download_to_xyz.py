from ase.io import write
from collections import defaultdict
from mp_api.client import MPRester
from multiprocessing import Pool
import numpy as np
import pickle
from pymatgen.io.ase import AseAtomsAdaptor

BATCH_SIZE = 25
KEYS = set()


def get_outcar_info(outcar):
    exclude = {"@module", "@class"}
    info = dict()
    for key in set(outcar) - exclude:
        if type(outcar[key]) == list and len(outcar[key]) == 0:
            pass
        elif type(outcar[key]) == list and type(outcar[key][0]) in [
            int,
            float,
        ]:
            info[f"outcar-{key}"] = " ".join([str(x) for x in outcar[key]])
        elif type(outcar[key]) == list and type(outcar[key][0]) == dict:
            mkeys = set(outcar[key][0])
            mdict = defaultdict(str)
            for i, row in enumerate(outcar[key]):
                for mkey in mkeys:
                    mdict[mkey] += f"{row[mkey]} "
            for mkey in mdict:
                info[f"outcar-{key}-{mkey}"] = mdict[mkey].rstrip()
        elif type(outcar[key]) == list and type(outcar[key][0]) == list:
            vals = np.array(outcar[key])
            vals = vals.reshape(vals.size, order="F")
            info[f"outcar-{key}"] = " ".join([str(val) for val in vals])
        else:
            info[f"outcar-{key}"] = outcar[key]
    return info


def get_output_info(output):
    info = dict()
    exclude = {
        "structure",
        "ionic_steps",
        "outcar",
        "crystal",
        "eigenvalues",
        "run_stats",
    }
    include = set(output) - exclude
    for key in include:
        if type(output[key]) == list and len(output[key]) > 0:
            pass
        elif type(output[key]) == list:
            vals = np.array(output[key])
            vals = vals.reshape(vals.size)
            info[f"output-{key}"] = " ".join([str(val) for val in vals])
        elif type(output[key]) == list and type(output[key][0]) == dict:
            mkeys = set(output[key][0])
            mdict = defaultdict(str)
            for i, row in enumerate(output[key]):
                for mkey in mkeys:
                    mdict[mkey] += f"{row[mkey]} "
            for mkey in mdict:
                info[f"output-{key}-{mkey}"] = mdict[mkey].rstrip()
        elif type(output[key]) == dict:
            mkeys = set(output[key])
            for mkey in mkeys:
                info[f"output-{key}-{mkey}"] = output[key][mkey]
        else:
            info[f"output-{key}"] = output[key]
    return info


def get_ionic_info(ionic_step):
    info = dict()
    if "e_fr_energy" in ionic_step:
        info["e_fr_energy"] = ionic_step["e_fr_energy"]
        info["e_wo_entrp"] = ionic_step["e_wo_entrp"]
        info["e_0_energy"] = ionic_step["e_0_energy"]
    else:
        info["e_fr_energy"] = ionic_step["electronic_steps"][-1]["e_fr_energy"]
        info["e_wo_entrp"] = ionic_step["electronic_steps"][-1]["e_wo_entrp"]
        info["e_0_energy"] = ionic_step["electronic_steps"][-1]["eentropy"]

    stress = np.array(ionic_step["stress"])
    stress = stress.reshape(stress.size, order="F")
    info["stress"] = " ".join([str(x) for x in stress])
    return info


def get_incar_info(j_calc):
    info = dict()
    incar = j_calc["input"]["incar"]
    exclude = {"run_stats"}
    for key in set(incar) - exclude:
        if type(incar[key]) == list:
            info[f"incar-{key}"] = " ".join([str(x) for x in incar[key]])
        else:
            info[f"incar-{key}"] = incar[key]
    return info


def convert(calc, task_id):
    atoms = []
    for j, j_calc in enumerate(calc):
        incar = get_incar_info(j_calc)
        output_info = get_output_info(j_calc["output"])
        outcar = get_outcar_info(j_calc["output"]["outcar"])
        for i, ionic_step in enumerate(j_calc["output"]["ionic_steps"]):
            a = AseAtomsAdaptor.get_atoms(ionic_step["structure"])
            a.info = dict(**get_ionic_info(ionic_step))
            a.info.update(outcar)
            a.info.update(incar)
            a.info.update(output_info)

            a.arrays["forces"] = np.array(
                calc[j]["output"]["ionic_steps"][i]["forces"]
            )

            a.info["material_id"] = D[task_id]["material_id"]
            a.info["task_id"] = task_id
            a.info["name"] = f"{task_id}-{j}-{i}"
            a.info["calc_type"] = D[task_id]["calc_type"]
            atoms.append(a)
            KEYS.update(set(a.info))
    return atoms


def mp_to_xyz(mpr, indices):
    atoms = []

    docs = mpr.tasks.search(
        task_ids=list(D.keys())[indices[0] : indices[1]],
        fields=["task_id", "calcs_reversed"],
    )

    for task_doc in docs:
        dct = task_doc.dict()
        atoms.extend(convert(dct["calcs_reversed"], dct["task_id"]))
    print(len(atoms))
    write(
        f"materials_project/mat_proj_xyz_files2/mp_{indices[0]}_{indices[1]}.xyz",
        atoms,
    )


if __name__ == "__main__":
    with open("materials_project/tasks_dict.pkl", "rb") as f:
        D = pickle.load(f)

    ids = list(D.keys())

    n_batches = len(ids) // BATCH_SIZE
    remain = len(ids) % BATCH_SIZE

    with MPRester(api_key="PFiAaUsvlnxNzJdOKoI5kWgEmo17rthx") as mpr:
        for b_num in range(12604, 12608):
            indices = (b_num * BATCH_SIZE, (b_num + 1) * BATCH_SIZE)
            print(indices)
            mp_to_xyz(mpr, indices)
            with open("mp_keys.txt", "r") as f:
                keys = set([x.rstrip() for x in f.readlines() if len(x) > 0])
            if keys >= KEYS:
                pass
            else:
                keys.update(KEYS)
                with open("mp_keys.txt", "w") as f:
                    for key in keys:
                        f.write(f"{key}\n")
        pass
        if remain:
            indices = (n_batches * BATCH_SIZE, len(ids))
            mp_to_xyz(mpr, indices)

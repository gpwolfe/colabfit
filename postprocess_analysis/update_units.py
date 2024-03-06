# from colabfit_utilities import get_client_notebook
from pymongo import MongoClient
import time
from datetime import datetime


def main():
    client = MongoClient(host="mongodb://10.32.250.13:30007")
    db = client["cf-update-2023-11-30"]
    # db = client['zif4']
    coll = db["property_instances"]
    results = []
    query_update = [
        # Energy
        # (
        # {"potential-energy.energy.source-unit": {"$in": ["Ha", "a.u.",
        #                                                  "Hartree"]}},
        #     {"$set": {"potential-energy.energy.source-unit": "hartree"}},
        #     "energy: a.u. to hartree",
        # ),
        # (
        #     {
        #         "atomization-energy.energy.source-unit": {
        #             "$in": ["Ha", "a.u.", "Hartree"]
        #         }
        #     },
        #     {"$set": {"atomization-energy.energy.source-unit": "hartree"}},
        #     "atomization energy: a.u. to hartree",
        # ),
        # (
        #     {"formation-energy.energy.source-unit": {"$in":
        #                                               ["Ha", "a.u.", "Hartree"]}},
        #     {"$set": {"formation-energy.energy.source-unit": "hartree"}},
        #     "formation energy: a.u. to hartree",
        # ),
        # (
        #     {"free-energy.energy.source-unit": {"$in": ["Ha", "a.u.", "Hartree"]}},
        #     {"$set": {"free-energy.energy.source-unit": "hartree"}},
        #     "free energy: a.u. to hartree",
        # ),
        #  Forces
        # (
        #     {
        #         "atomic-forces.forces.source-unit": {
        #             "$in": [
        #                 "kcal/mol/A",
        #                 "kcal/mol angstrom",
        #                 "kcal/mol Angstrom",
        #                 "kcal/mol A",
        #                 "kcal/mol/Ang",
        #                 "kcal/molAng",
        #             ]
        #         }
        #     },
        #     {"$set": {"atomic-forces.forces.source-unit": "kcal/mol/angstrom"}},
        #     "forces: kcal/mol/A etc. to kcal/mol/angstrom",
        # # ),
        # (
        #     {
        #         "atomic-forces.forces.source-unit": {
        #             "$in": ["eV/A", "eV/Ang", "meV Å^-1"]
        #         }
        #     },
        #     {"$set": {"atomic-forces.forces.source-unit": "eV/angstrom"}},
        #     "forces eV/A, Ang to eV/angstrom",
        # ),
        # (
        #     {"atomic-forces.forces.source-unit": {"$in": ["Hartree/A", "Ha/A"]}},
        #     {"$set": {"atomic-forces.forces.source-unit": "hartree/angstrom"}},
        #     "forces Hartree(Ha)/A to hartree/angstrom",
        # ),
        # # Stress
        # (
        #     {"cauchy-stress.stress.source-unit": {"$in": ["eV/Ang^3"]}},
        #     {"$set": {"cauchy-stress.stress.source-unit": "eV/angstrom^3"}},
        #     "stress eV/Ang^3 to eV/angstrom^3",
        # ),
        # 20.02.2024 -- Fixing DS_e94my2wrh074_0  mbGDML_maldonado_2023 energy units
        # (
        #     {
        #         "atomic-forces.forces.source-unit": {
        #             "$in": ["eV/A", "eV/Ang", "meV Å^-1"]
        #         }
        #     },
        #     {"$set": {"atomic-forces.forces.source-unit": "eV/angstrom"}},
        #     "forces eV/A, Ang to eV/angstrom",
        # ),
        # (
        #     {
        #         "relationships": "DS_e94my2wrh074_0",
        #         "potential-energy.energy.source-unit.field": "e_unit",
        #     },
        #     {"$set": {"potential-energy.energy.source-unit": "kcal/mol"}},
        # ),
        # (
        #     {
        #         "relationships": "DS_caktb6z8yiy7_0",
        #         "potential-energy.energy.source-unit.value": "eV",
        #     },
        #     {"$set": {"potential-energy.energy.source-unit": "eV"}},
        # ),
        # (
        #     {
        #         "relationships": "DS_02cqe6a0bobu_0",
        #         "atomic-forces.forces.source-unit": "eV/angstrom",
        #     },
        #     {"$set": {"atomic-forces.forces.source-unit": "hartree/angstrom"}},
        # ),
        # (
        #     {
        #         "relationships": "DS_9in0wrvt6qg2_0",
        #         "cauchy-stress.stress.source-unit": "a.u.",
        #     },
        #     {"$set": {"cauchy-stress.stress.source-unit": "hartree/bohr"}},
        # ),
        (
            {
                "relationships.dataset": "DS_a0bxs66goqvv_0",
                "potential-energy.energy.source-unit": "hartree",
            },
            {"$set": {"potential-energy.energy.source-unit": "eV"}},
        ),
        (
            {
                "relationships": "DS_a0bxs66goqvv_0",
                "atomic-forces.forces.source-unit": "Hartree/Bohr",
            },
            {"$set": {"atomic-forces.forces.source-unit": "eV/angstrom"}},
        ),
        (
            {
                "relationships": "DS_mc8p14cpn2ea_0",
                "cauchy-stress.stress.source-unit": "eV/atom",
            },
            {"$set": {"cauchy-stress.stress.source-unit": "eV/angstrom"}},
        ),
    ]
    timestamp = time.time()
    now = datetime.fromtimestamp(timestamp)
    with open("update_units_results.txt", "a") as f:

        f.write(f"{now}\n")
        f.write(f"{db.name}\t{coll.name}\n")
    for i, q_u in enumerate(query_update):
        query = q_u[0]
        update = q_u[1]
        print(query, update)
        res = coll.update_many(query, update)
        with open("update_units_results.txt", "a") as f:
            f.write(f"{q_u}\t{res.raw_result}\n")
        results.append((q_u, res))
    return results


if __name__ == "__main__":
    main()

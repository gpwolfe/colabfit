"""
author:gpwolfe

Data can be downloaded from:
https://github.com/jmbowma/QM-22

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy

Other properties added to metadata
----------------------------------

File notes
----------

"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import potential_energy_pd

from pathlib import Path
import sys

DATASET_FP = Path("QM-22-main")
DATASET = "QM-22"

LINKS = [
    "https://github.com/jmbowma/QM-22",
    "https://doi.org/10.1063/5.0089200",
]
AUTHORS = [
    "Joel M. Bowman",
    "Chen Qu",
    "Riccardo Conte",
    "Apurba Nandi",
    "Paul L. Houston",
    "Qi Yu",
]
DS_DESC = "Includes CHON molecules of 4-15 atoms, developed in counterpoint\
 to the MD17 dataset, run at higher total energies (above 500 K) and with a\
 broader configuration space."
ELEMENTS = ["C", "H", "O", "N"]
GLOB_STR = "*.xyz"

SOFT_METH = {
    "Acetaldehyde_singlet": {"software": "Molpro", "method": "CCSD(T)/AVTZ"},
    "Acetaldehyde_triplet": {
        "software": "Molpro",
        "method": "RCCSD(T)/cc-pVTZ,",
    },
    "Ethanol": {"software": "MSA", "method": "DFT(B3LYP)"},
    "Formic_acid_dimer": {
        "software": "MULTIMODE",
        "method": "CCSD(T)-F12a/haTZ",
    },
    "Glycine": {"software": "Molpro", "method": "DFT(B3LYP)/aug-cc-pVDZ"},
    "H2CO_and_HCOH": {"software": "Molpro", "method": "MRCI"},
    "Hydronium": {"software": "Molpro", "method": "CCSD(T)"},
    "Malonaldehyde": {"software": "MULTIMODE", "method": "CCSD(T)"},
    "Methane": {"software": "MSA", "method": "DFT(B3LYP)/6-31+G(d)"},
    "N-methylacetamide": {
        "software": "Molpro",
        "method": "DFT(B3LYP)/cc-pVDZ",
    },
    "OCHCO_cation": {"software": "Molpro", "method": "CCSD(T)"},
    "syn-CH3CHOO": {
        "software": "MESMER",
        "method": "CCSD(T)/aug-cc-pVTZ/M06-ZX",
    },
    "Tropolone": {"software": "Molpro", "method": "DFT(B3LYP)/6-31+G(d)"},
}


def reader(filepath):
    name = filepath.stem
    atoms = read(filepath, index=":")
    configs = []
    if name != "Malonaldehyde":
        for i, config in enumerate(atoms):
            energy = list(config.info.keys())[0]
            config.info = {"energy": float(energy)}
            config.info["name"] = f"{name}_{i}"
            configs.append(config)
    elif name == "Malonaldehyde":
        # Malonaldehyde has CBS energy listed: ref, corr(CCSD), corr(T), tot
        for i, config in enumerate(atoms):
            e_ref, e_corr_ccsd, e_corr_t, energy = list(config.info.keys())
            config.info["e_ref"] = e_ref
            config.info["e_corr_ccsd"] = e_corr_ccsd
            config.info["e_corr_t"] = e_corr_t
            config.info["energy"] = float(energy)
            config.info["name"] = f"{name}_{i}"
            configs.append(config)

    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")
    client.insert_property_definition(potential_energy_pd)
    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string="Tropolone.xyz",
        generator=False,
    )

    metadata = {
        "software": {"value": SOFT_METH["Tropolone"]["software"]},
        "method": {"value": SOFT_METH["Tropolone"]["method"]},
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "Hartree"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ]
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            # generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))
    all_co_ids = list(all_co_ids)
    all_do_ids = list(all_do_ids)

    desc = f"Tropolone configurations from {DATASET} dataset."
    name = "QM-22-Tropolone"
    cs_id = client.insert_configuration_set(
        # Using all_co_ids here bc they contain only Tropolone data
        all_co_ids,
        description=desc,
        name=name,
    )
    cs_ids = []
    cs_ids.append(cs_id)

    for path in DATASET_FP.glob(GLOB_STR):
        name = path.stem
        if name == "Tropolone":
            pass
        else:
            configurations = load_data(
                file_path=DATASET_FP,
                file_format="folder",
                name_field="name",
                elements=ELEMENTS,
                reader=reader,
                glob_string=f"{name}.xyz",
                generator=False,
            )
            if name == "Malonaldehyde":
                metadata = {
                    "software": {"value": SOFT_METH[name]["software"]},
                    "method": {"value": SOFT_METH[name]["method"]},
                    "ref-energy": {"field": "e_ref"},
                    "correlation-energy-ccsd": {"field": "e_corr_ccsd"},
                    "correlation-energy-t": {"field": "e_corr_t"},
                }
            else:
                metadata = {
                    "software": {"value": SOFT_METH[name]["software"]},
                    "method": {"value": SOFT_METH[name]["method"]},
                }
            property_map = {
                "potential-energy": [
                    {
                        "energy": {"field": "energy", "units": "Hartree"},
                        "per-atom": {"value": False, "units": None},
                        "_metadata": metadata,
                    }
                ]
            }
            ids = list(
                client.insert_data(
                    configurations,
                    property_map=property_map,
                    # generator=False,
                    verbose=True,
                )
            )
            co_ids, do_ids = list(zip(*ids))
            all_co_ids.extend(co_ids)  # This is not used again
            all_do_ids.extend(do_ids)

            cset_name = f"{DATASET}_{name}"
            desc = f"{name} configurations from {DATASET} dataset"

            print(
                "Configuration set: ",
                f"({name}):".rjust(22),
                f"{len(co_ids)}".rjust(7),
            )
            if len(co_ids) > 0:
                cs_id = client.insert_configuration_set(
                    co_ids, description=desc, name=cset_name
                )

                cs_ids.append(cs_id)
            else:
                pass

    client.insert_dataset(
        cs_ids=cs_ids,
        pr_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

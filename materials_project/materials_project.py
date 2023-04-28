from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    cauchy_stress_pd,
    free_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("mat_proj_xyz_files_expanded_data")

elements = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
]


def reader(file_path):
    atom = read(file_path, index=":")
    return atom


def main(ip, fileset: list, dataset_id=None, config_set_id=None):
    client = MongoDatabase("----", uri=f"mongodb://{ip}:27017")
    configurations = []
    for path in fileset:
        configs = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=elements,
            reader=reader,
            glob_string=path.name,
            generator=False,
        )
        configurations += configs

    # Skip if dataset has already been created
    if dataset_id is None:
        for pd in [
            atomic_forces_pd,
            cauchy_stress_pd,
            free_energy_pd,
        ]:
            client.insert_property_definition(pd)

    metadata = {
        "software": {"value": "VASP"},
        "method": {"field": "calc_type"},
        "material-id": {"field": "material_id"},
        "internal_energy": {"field": "e_0_energy"},
    }
    # excluded keys are included under other names or in property_map
    exclude = {"calc_type", "e_fr_energy", "forces", "stress", "material_id"}
    metadata.update({k: {"field": k} for k in KEYS if k not in exclude})
    property_map = {
        "free-energy": [
            {
                "energy": {"field": "e_fr_energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": metadata,
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "stress", "units": "eV/A"},
                "_metadata": metadata,
            }
        ],
    }
    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    # Create a dataset/config set if none exists, otherwise reuse MP dataset
    # Currently, update_dataset requires configurations sets to be updated
    # as well.
    if dataset_id is None:

        dataset_id = client.insert_dataset(
            do_hashes=all_do_ids,
            name="Materials Project",
            authors=[
                "A. Jain",
                "S.P. Ong",
                "G. Hautier",
                "W. Chen",
                "W.D. Richards",
                "S. Dacek",
                "S. Cholia",
                "D. Gunter",
                "D. Skinner",
                "G. Ceder",
                "K.A. Persson",
            ],
            links=[
                "https://materialsproject.org/",
            ],
            description="Configurations from the Materials Project database:"
            " an online resource with the goal of computing properties of all"
            " inorganic materials.",
            verbose=True,
        )
        return dataset_id
    else:

        dataset_id = client.update_dataset(
            ds_id=dataset_id, add_do_ids=all_do_ids
        )
    return dataset_id


KEYS = [
    "outcar-ngf",
    "output-is_metal",
    "output-vbm",
    "incar-ENAUG",
    "output-epsilon_static_wolfe",
    "task_id",
    "incar-ISIF",
    "incar-SMASS",
    "output-bandgap",
    "outcar-p_ion",
    "outcar-electrostatic_potential",
    "output-epsilon_static",
    "output-projected_eigenvalues",
    "calc_type",
    "incar-AMIN",
    "incar-IBRION",
    "material_id",
    "incar-AMIX_MAG",
    "incar-NSW",
    "outcar-magnetization-d",
    "output-locpot-1",
    "e_fr_energy",
    "incar-ISTART",
    "incar-LEFG",
    "incar-NGZ",
    "incar-LDAUU",
    "outcar-p_elec",
    "incar-LDAUTYPE",
    "outcar-sampling_radii",
    "incar-LRHFATM",
    "output-locpot-2",
    "incar-PREC",
    "outcar-nelect",
    "output-eigenvalue_band_properties-bandgap",
    "incar-LDAUJ",
    "outcar-p_sp2",
    "output-energy",
    "outcar-run_stats",
    "name",
    "output-locpot-0",
    "outcar-charge-s",
    "outcar-charge-p",
    "incar-METAGGA",
    "incar-SYMPREC",
    "incar-LELF",
    "outcar-onsite_density_matrices-1",
    "outcar-charge-d",
    "incar-NELMDL",
    "incar-LMAXTAU",
    "incar-LREAL",
    "incar-QUAD_EFG",
    "incar-NGY",
    "incar-BMIX_MAG",
    "outcar-onsite_density_matrices--1",
    "incar-LWAVE",
    "incar-NPAR",
    "incar-BMIX",
    "incar-KPAR",
    "incar-GGA",
    "output-is_gap_direct",
    "output-eigenvalue_band_properties-vbm",
    "outcar-magnetization-s",
    "output-efermi",
    "output-epsilon_ionic",
    "outcar-drift",
    "e_wo_entrp",
    "incar-SYSTEM",
    "outcar-zval_dict",
    "incar-ALGO",
    "incar-EDIFF",
    "output-energy_per_atom",
    "stress",
    "incar-LDAU",
    "incar-ISYM",
    "incar-ADDGRID",
    "outcar-magnetization-f",
    "e_0_energy",
    "incar-EDIFFG",
    "outcar-efermi",
    "output-direct_gap",
    "incar-POTIM",
    "output-eigenvalue_band_properties-cbm",
    "incar-LORBIT",
    "output-cbm",
    "incar-IMIX",
    "incar-ISMEAR",
    "output-final_energy",
    "outcar-magnetization-p",
    "outcar-is_stopped",
    "incar-LAECHG",
    "outcar-magnetization-tot",
    "incar-LCHARG",
    "incar-LVTOT",
    "incar-SIGMA",
    "incar-MAGMOM",
    "incar-NELMIN",
    "incar-LASPH",
    "incar-KSPACING",
    "incar-ISPIN",
    "outcar-charge-tot",
    "incar-LDAUPRINT",
    "incar-KPOINT_BSE",
    "incar-LMAXMIX",
    "outcar-charge-f",
    "incar-NCORE",
    "incar-LMIXTAU",
    "incar-LPEAD",
    "output-eigenvalue_band_properties-is_gap_direct",
    "incar-NBANDS",
    "incar-ICHARG",
    "incar-EFIELD_PEAD",
    "incar-LDAUL",
    "incar-LCALCEPS",
    "incar-NELM",
    "incar-AMIX",
    "output-final_energy_per_atom",
    "outcar-total_magnetization",
    "incar-LCALCPOL",
    "outcar-p_sp1",
    "incar-NGX",
    "incar-ENCUT",
]

if __name__ == "__main__":
    batch_size = 1
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(sys.argv[1:])
    ip = args.ip

    files = list(DATASET_FP.glob("*.xyz"))

    # Import by batch, with first batch returning dataset-id and config-set-id
    n_batches = len(files) // batch_size
    batch_1 = files[:batch_size]
    dataset_id = main(ip, batch_1, None, None)
    for n in range(1, n_batches):
        batch_n = files[batch_size * n : batch_size * (n + 1)]
        dataset_id = main(
            ip,
            batch_n,
            dataset_id=dataset_id,
        )
        print(f"Dataset: {dataset_id}")
    if len(files) % batch_size and len(files) > batch_size:
        batch_n = files[batch_size * n_batches :]
        dataset_id = main(ip, batch_n, dataset_id)

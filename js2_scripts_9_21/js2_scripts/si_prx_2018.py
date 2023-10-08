"""
author: Gregory Wolfe, Alexander Tao

Properties
----------


info keys:

#  'DFT_energy',
#  'DFT_virial',
 'Minim_Constant_Volume',
 'Minim_Hydrostatic_Strain',
 'Minim_Lattice_Fix',
 'calculate_stress',
 'castep_file_name',
 'castep_run_time',
 'config_type',
 'continuation',
 'cut_off_energy',
 'cutoff',
 'cutoff_factor',
 'data_distribution',
#  'dft_energy',
#  'dft_virial',
 'elec_energy_tol',
#  'energy',
 'enthalpy',
 'fine_grid_scale',
 'finite_basis_corr',
 'fix_occupancy',
#  'free_energy',
#  'gap_energy',
#  'gap_virial',
 'grid_scale',
 'hamiltonian',
 'i_step',
 'in_file',
 'iprint',
#  'kinetic_energy',
 'kpoints_mp_grid',
 'max_scf_cycles',
 'md_cell_t',
 'md_delta_t',
 'md_ensemble',
 'md_ion_t',
 'md_num_iter',
 'md_temperature',
 'md_thermostat',
 'mix_charge_amp',
 'mix_history_length',
 'mixing_scheme',
 'name',
 'nextra_bands',
 'nneightol',
 'num_dump_cycles',
 'opt_strategy',
 'opt_strategy_bias',
 'popn_calculate',
 'pressure',
 'reuse',
 'smearing_width',
 'spin_polarized',
 'task',
 'temperature',
 'time',
#  'virial',
 'xc_functional'

array keys:

#  'DFT_force',
 'acc',
 'avg_ke',
 'avgpos',
 'damp_mask',
#  'dft_force',
 'force_ewald',
 'force_locpot',
 'force_nlpot',
#  'forces',
 'frac_pos',
#  'gap_force',
 'map_shift',
 'mass',
 'masses',
 'momenta',
 'n_neighb',
 'numbers',
 'oldpos',
 'positions',
 'thermostat_region',
 'travel',
 'velo'

force keys:

DFT_force,
dft_force
forces
gap_force

stress keys

virial
gap_virial
dft_virial
DFT_virial

energy keys

dft_energy
gap_energy
free_energy
DFT_energy
kinetic_energy

other
'cut_off_energy',


File notes
----------
File appears to be incomplete, with just the number of atoms and part of the "Lattice="
for the next configuration header. This has been deleted for the reader function.

"""

from argparse import ArgumentParser
from ase.io import read
from pathlib import Path
import sys

from colabfit.tools.database import generate_ds_id, load_data, MongoDatabase
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    free_energy_pd,
    potential_energy_pd,
)

DATASET_FP = Path("/large_data/new_raw_datasets/Si_Berk/Si.extxyz")
DATASET_FP = Path().cwd().parent / "data/berk_si"
DS_NAME = "Si_PRX_2018"
DS_DESC = (
    "This dataset was intended to allow accurate energy estimates for a "
    "material (silicon) by interpolating a dataset that covers the entire range of "
    "physically relevant configurations. "
    "The dataset was built over an extended period, using multiple computational "
    "facilities. The kinds of configuration that included are chosen using "
    "intuition and past experience to guide what needs to be included to obtain "
    "good coverage pertaining to a range of properties. The number of configurations "
    "in the final database is a result of somewhat ad hoc choices, driven partly by "
    "the varying computational cost of the electronic-structure calculation and partly "
    "by observed success in predicting properties, signaling a sufficient amount of "
    "data. Each configuration yields a total energy, six components of the stress "
    "tensor, and three force components for each atom."
)
AUTHORS = ["Albert P. Bartók", "James Kermode", "Noam Bernstein", "Gábor Csányi"]
LINKS = [
    "https://doi.org/10.1063/1.4990503",
    "https://github.com/DescriptorZoo/sensitivity-dimensionality-results",
]
GLOB = "Si.extxyz"

PI_MD = {
    "software": {"value": "CASTEP"},
    "method": {"value": "DFT-PW91"},
    "energy-cutoff": {"value": "250 eV"},
    "temperature": {"field": "temperature"},
    "kpoints": {"field": "kpoints"},
}

property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        },
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/A"},
            "_metadata": PI_MD,
        }
    ],
    # "kinetic-energy": [
    #     {
    #         "energy": {"field": "kinetic_energy", "units": "eV"},
    #         "_metadata": PI_MD,
    #     }
    # ],
    "free-energy": [
        {
            "energy": {"field": "free_energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_MD,
        }
    ],
}


def reader(fp):
    name = fp.stem
    configs = read(fp, ":-1")
    for i, config in enumerate(configs):
        config.info["name"] = f"{name}_{i}"
        forces = config.arrays.get(
            "forces", config.arrays.get("dft_force", config.arrays.get("DFT_force"))
        )
        # gap_force = config.arrays.get("gap_force")
        if forces is not None:
            config.info["forces"] = forces
        # if gap_force is not None:
        #     config.info["gap_force"] = gap_force
        kpoints = config.info.get("kpoints_mp_grid")
        if kpoints is not None:
            kpoints = " x ".join(kpoints.astype(str))
            config.info["kpoints"] = kpoints
        energy = config.info.get(
            "energy", config.info.get("dft_energy", config.info.get("DFT_energy"))
        )
        if energy is not None:
            config.info["energy"] = energy
        # gap_energy = config.info.get("gap_energy")

        virial = config.info.get(
            "virial", config.info.get("dft_virial", config.info.get("DFT_virial"))
        )
        if virial is not None:
            config.info["virial"] = virial
    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="cf-test",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        reader=reader,
        elements=["Si"],
        verbose=True,
        generator=False,
        glob_string=GLOB,
    )

    # kinetic_energy, energy, forces, free_energy

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(free_energy_pd)

    # kinetic_energy_property_definition = {
    #     "property-id": "kinetic-energy",
    #     "property-name": "kinetic-energy",
    #     "property-title": "kinetic-energy",
    #     "property-description": "kinetic energy",
    #     "energy": {
    #         "type": "float",
    #         "has-unit": True,
    #         "extent": [],
    #         "required": True,
    #         "description": "kinetic energy",
    #     },
    # }

    # client.insert_property_definition(kinetic_energy_property_definition)

    ids = list(
        client.insert_data(
            configurations,
            co_md_map=CO_MD,
            ds_id=ds_id,
            property_map=property_map,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        ds_id=ds_id,
        name=DS_NAME,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


CO_MD = {
    key: {"field": key}
    for key in [
        #  'DFT_energy',
        #  'DFT_virial',
        "Minim_Constant_Volume",
        "Minim_Hydrostatic_Strain",
        "Minim_Lattice_Fix",
        "calculate_stress",
        "castep_file_name",
        "castep_run_time",
        "config_type",
        "continuation",
        "cut_off_energy",
        "cutoff",
        "cutoff_factor",
        "data_distribution",
        #  'dft_energy',
        #  'dft_virial',
        "elec_energy_tol",
        #  'energy',
        "enthalpy",
        "fine_grid_scale",
        "finite_basis_corr",
        "fix_occupancy",
        #  'free_energy',
        "gap_energy",
        "gap_virial",
        "grid_scale",
        "hamiltonian",
        "i_step",
        "in_file",
        "iprint",
        #  'kinetic_energy',
        "kpoints_mp_grid",
        "max_scf_cycles",
        "md_cell_t",
        "md_delta_t",
        "md_ensemble",
        "md_ion_t",
        "md_num_iter",
        "md_temperature",
        "md_thermostat",
        "mix_charge_amp",
        "mix_history_length",
        "mixing_scheme",
        "name",
        "nextra_bands",
        "nneightol",
        "num_dump_cycles",
        "opt_strategy",
        "opt_strategy_bias",
        "popn_calculate",
        "pressure",
        "reuse",
        "smearing_width",
        "spin_polarized",
        "task",
        "temperature",
        "time",
        #  'virial',
        "xc_functional"
        #  'DFT_force',
        "acc",
        "avg_ke",
        "avgpos",
        "damp_mask",
        #  'dft_force',
        "force_ewald",
        "force_locpot",
        "force_nlpot",
        #  'forces',
        "frac_pos",
        "gap_force",
        "map_shift",
        "mass",
        "masses",
        "momenta",
        "n_neighb",
        "numbers",
        "oldpos",
        "positions",
        "thermostat_region",
        "travel",
        "velo",
    ]
}

if __name__ == "__main__":
    main(sys.argv[1:])

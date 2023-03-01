"""
author:gpwolfe

Data can be downloaded from:
https://archive.materialscloud.org/record/2022.92

File address:
https://archive.materialscloud.org/record/file?filename=training.zip&record_id=1411

Extract file to a new parent directory before running script.
mkdir <project_dir>/scripts/hpt_nc_2022
unzip training.zip "*.out" -d <project_dir>/scripts/hpt_nc_2022/

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 HPt_nc_2022.py -i (or --ip) <database_ip>
"""
from argparse import ArgumentParser
import ase
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from typing import List
import numpy as np
from pathlib import Path
from pymongo.errors import OperationFailure
import sys

DATASET_FP = Path("scripts/hpt_nc_2022/training")


def reader(filepath):
    atoms = OtfAnalysis(filepath, calculate_energy=True)
    ase_atoms = atoms.output_md_structures()
    return ase_atoms


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", uri=f"mongodb://{args.ip}:27017")

    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=["H", "Pt"],
        reader=reader,
        glob_string="*.out",
        generator=False,
    )

    metadata = {"software": {"value": "LAMMPS"}, "method": {"value": "DFT"}}

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
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

    cs_regexes = [
        [
            "All_H2/Pt(III)",
            ".*",
            "All configurations from H/Pt(III)",
        ],
        [
            "H2_H2/Pt(III)",
            "H2*",
            "H2 configurations from H/Pt(III)",
        ],
        [
            "Pt-bulk_H2/Pt(III)",
            "Pt-bulk*",
            "Pt-bulk configurations from H/Pt(III)",
        ],
        [
            "Pt-surface_H2/Pt(III)",
            "Pt-surface*",
            "Pt-surface configurations from H/Pt(III)",
        ],
        [
            "PtH_H2/Pt(III)",
            "PtH*",
            "PtH configurations from H/Pt(III)",
        ],
    ]

    cs_ids = []

    for i, (name, regex, desc) in enumerate(cs_regexes):
        try:
            co_ids = client.get_data(
                "configurations",
                fields="hash",
                query={
                    "hash": {"$in": all_co_ids},
                    "names": {"$regex": regex},
                },
                ravel=True,
            ).tolist()
        except OperationFailure:
            print(f"No match for regex: {regex}")
            continue

        print(
            f"Configuration set {i}",
            f"({name}):".rjust(25),
            f"{len(co_ids)}".rjust(7),
        )

        if len(co_ids) == 0:
            pass
        else:
            cs_id = client.insert_configuration_set(
                co_ids, description=desc, name=name
            )

            cs_ids.append(cs_id)

    client.insert_dataset(
        cs_ids,
        all_do_ids,
        name="HPt_nc_2022",
        authors=["S. Lee, K. Ermanis, J.M. Goodman"],
        links=[
            "https://doi.org/10.24435/materialscloud:r0-84",
            "https://doi.org/10.1038/s41467-022-32294-0",
        ],
        description="A training dataset of 90,000 configurations"
        " with interaction properties between H2 and Pt(111) surfaces.",
        verbose=True,
    )


# From the Flare repository -- Flare install failed, so this is necessary code


def get_header_item(line, header_info, kw):
    if not isinstance(line, str):
        return

    pattern = header_dict[kw][0]
    value_type = header_dict[kw][1]

    if header_dict[kw][2]:
        pattern = pattern.lower()
        line = line.lower()

    if pattern in line:
        header_info[kw] = value_type(line.split(":")[1].strip())


header_dict = {
    "restart": ["Restart", int, False],
    "frames": ["Frames", int, True],
    "atoms": ["Number of atoms", int, True],
    "dt": ["Timestep", float, True],
}


def parse_header_information(lines) -> dict:
    """
    Get information about the run from the header of the file
    :param outfile:
    :return:
    """
    header_info = {}

    for i, line in enumerate(lines):
        line_lower = line.lower()

        for kw in header_dict:
            get_header_item(line, header_info, kw)

        if "system species" in line_lower:
            line = line.split(":")[1]
            line = line.split("'")
            species = [item for item in line if item.isalpha()]
            header_info["species_set"] = set(species)
        if "periodic cell" in line_lower:
            vectors = []
            for cell_line in lines[i + 1 : i + 4]:
                cell_line = cell_line.strip().replace("[", "").replace("]", "")
                vec = cell_line.split()
                vector = [float(vec[0]), float(vec[1]), float(vec[2])]
                vectors.append(vector)
            header_info["cell"] = np.array(vectors)
        if "previous positions" in line_lower:
            struc_spec = []
            prev_positions = []
            for pos_line in lines[i + 1 : i + 1 + header_info.get("atoms", 0)]:
                pos = pos_line.split()
                struc_spec.append(pos[0])
                prev_positions.append(
                    (float(pos[1]), float(pos[2]), float(pos[3]))
                )
            header_info["species"] = struc_spec
            header_info["prev_positions"] = np.array(prev_positions)

    return header_info


class OtfAnalysis:
    """
    Parse the OTF log file to get trajectory, training data,
    thermostat, and build GP model.

    Args:
        filename (str): name of the OTF log file.
        calculate_energy (bool): if the potential energy is computed and
            needs to be parsed, then set to True. Default False.
    """

    def __init__(self, filename, calculate_energy=False):
        self.filename = Path(filename)
        self.calculate_energy = calculate_energy

        blocks = split_blocks(filename)

        self.header = parse_header_information(blocks[0])
        self.noa = self.header["atoms"]
        # self.noh = self.header["n_hyps"]

        self.position_list = []
        self.cell_list = []
        self.force_list = []
        self.stress_list = []
        self.uncertainty_list = []
        self.velocity_list = []
        self.temperatures = []
        self.dft_frames = []
        self.dft_times = []
        self.times = []
        self.energies = []
        self.thermostat = {}

        self.gp_position_list = []
        self.gp_cell_list = []
        self.gp_force_list = []
        self.gp_stress_list = []
        self.gp_uncertainty_list = []
        self.gp_velocity_list = []
        self.gp_atom_list = []
        self.gp_species_list = []
        self.gp_atom_count = []
        self.gp_thermostat = {}

        # self.gp_hyp_list = [self.header["hyps"]]

        # self.mae_list = []
        # self.mav_list = []

        self.parse_pos_otf(blocks[1:])

        if self.calculate_energy:
            self.energies = self.thermostat["potential energy"]

    def parse_pos_otf(self, blocks):
        """
        Exclusively parses MD run information
        :param filename:
        :return:
        """
        # print("parse 0tf") used
        n_steps = len(blocks) - 1

        for block in blocks:
            for index, line in enumerate(block):
                # DFT frame
                if line.startswith("*-Frame"):
                    dft_frame_line = line.split()
                    self.dft_frames.append(int(dft_frame_line[1]))
                    dft_time_line = block[index + 1].split()
                    self.dft_times.append(float(dft_time_line[-2]))

                    append_atom_lists(
                        self.gp_species_list,
                        self.gp_position_list,
                        self.gp_force_list,
                        self.gp_uncertainty_list,
                        self.gp_velocity_list,
                        block,
                        index,
                        self.noa,
                        True,
                        # self.noh,
                    )

                    post_frame = block[index + 3 + self.noa :]

                # MD frame
                if line.startswith("-Frame"):
                    n_steps += 1
                    time_line = block[index + 1].split()
                    sim_time = float(time_line[2])
                    self.times.append(sim_time)

                    # TODO: generalize this to account for arbitrary starting list
                    append_atom_lists(
                        [],
                        self.position_list,
                        self.force_list,
                        self.uncertainty_list,
                        self.velocity_list,
                        block,
                        index,
                        self.noa,
                        False,
                        # self.noh,
                    )

                    post_frame = block[index + 3 + self.noa :]
                    extract_global_info(
                        self.cell_list,
                        self.stress_list,
                        self.thermostat,
                        post_frame,
                    )

    def get_msds(self):
        msds = []
        for pos in self.position_list:
            msds.append(np.mean((pos - self.position_list[0]) ** 2))
        return msds

    def output_md_structures(self):
        """
        Returns structure objects corresponding to the MD frames of an OTF run.
        :return:
        """

        structures = []
        cell = self.header["cell"]
        species = self.header["species"]
        for i in range(len(self.position_list)):
            if not self.calculate_energy:
                energy = 0
            else:
                energy = self.energies[i]

            cur_struc = ase.Atoms(
                cell=cell,
                symbols=species,
                positions=self.position_list[i],
            )
            cur_struc.info["forces"] = np.array(self.force_list[i])
            cur_struc.stds = np.array(self.uncertainty_list[i])
            cur_struc.info["energy"] = energy
            cur_struc.info["name"] = f"{self.filename.parts[-2]}_{i}"
            cur_struc.stress = self.stress_list[i]
            structures.append(cur_struc)
        return structures


def split_blocks(filename):
    with open(filename, "r") as f:
        lines = f.readlines()
        head = 0
        blocks = []
        for index, line in enumerate(lines):
            if "*-Frame" in line or line.startswith("---"):
                blocks.append(lines[head:index])
                head = index
    return blocks


def append_atom_lists(
    species_list: List[str],
    position_list: List[np.ndarray],
    force_list: List[np.ndarray],
    uncertainty_list: List[np.ndarray],
    velocity_list: List[np.ndarray],
    lines: List[str],
    index: int,
    noa: int,
    dft_call: bool,
    # noh: int,
) -> None:
    # print('append atom lists')  used
    """Update lists containing atom information at each snapshot."""

    if lines[0].startswith("---"):
        start_index = 4
    else:
        start_index = 3

    noa = 0
    for line in lines[start_index:]:
        if line.strip():
            noa += 1
        else:
            break

    species, positions, forces, uncertainties, velocities = parse_snapshot(
        lines, index, noa, dft_call
    )

    species_list.append(species)
    position_list.append(positions)
    force_list.append(forces)
    uncertainty_list.append(uncertainties)
    velocity_list.append(velocities)


def parse_snapshot(
    lines,
    index,
    noa,
    dft_call,
):
    """Parses snapshot of otf output file."""
    # print('parse snapshot')  used
    # initialize values
    species = []
    positions = np.zeros((noa, 3))
    forces = np.zeros((noa, 3))
    uncertainties = np.zeros((noa, 3))
    velocities = np.zeros((noa, 3))

    # Current setting for # of lines to skip after Frame marker
    skip = 3

    for count, frame_line in enumerate(
        lines[(index + skip) : (index + skip + noa)]
    ):
        # parse frame line
        spec, position, force, uncertainty, velocity = parse_frame_line(
            frame_line
        )

        # update values
        species.append(spec)
        positions[count] = position
        forces[count] = force
        uncertainties[count] = uncertainty
        velocities[count] = velocity

    return species, positions, forces, uncertainties, velocities


def parse_frame_line(frame_line):
    """parse a line in otf output.
    :param frame_line: frame line to be parsed
    :type frame_line: string
    :return: species, position, force, uncertainty, and velocity of atom
    :rtype: list, np.arrays
    """
    # print("parse_frame") used
    frame_line = frame_line.split()

    spec = str(frame_line[0])
    position = np.array([float(n) for n in frame_line[1:4]])
    force = np.array([float(n) for n in frame_line[4:7]])
    uncertainty = np.array([float(n) for n in frame_line[7:10]])
    velocity = np.array([float(n) for n in frame_line[10:13]])

    return spec, position, force, uncertainty, velocity


def extract_global_info(
    cell_list,
    stress_list,
    thermostat,
    block,
):
    # print("extract global info") used
    for ind, line in enumerate(block):
        if "cell" in line:
            vectors = []
            for cell_line in block[ind + 1 : ind + 4]:
                cell_line = cell_line.strip().replace("[", "").replace("]", "")
                vec = cell_line.split()
                vector = [float(vec[0]), float(vec[1]), float(vec[2])]
                vectors.append(vector)
            cell_list.append(vectors)
        if "Stress" in line:
            vectors = []
            stress_line = block[ind + 2].replace("-", " -").split()
            vectors = [float(s) for s in stress_line]
            stress_list.append(vectors)

        for t in [
            "Pressure",
            "Temperature",
            "Kinetic energy",
            "Potential energy",
            "Total energy",
        ]:
            get_thermostat(thermostat, t, line)


def get_thermostat(thermostat, kw, line):
    # used
    kw = kw.lower()
    line = line.lower()
    if kw in line:
        try:
            value = float(line.split()[-2])  # old style
        except:
            value = float(line.split()[-1])  # new style
        if kw in thermostat:
            thermostat[kw].append(value)
        else:
            thermostat[kw] = [value]


if __name__ == "__main__":
    main(sys.argv[1:])

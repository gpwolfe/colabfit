"""
author:gpwolfe

Data can be downloaded from:

Download link:


Extract to project folder and rename file
tar -xvzf SIMPLE-NN.tar.gz -C $project_dir/scripts/simple_nn \
    SIMPLE-NN/examples/SiO2/ab_initio_output/OUTCAR_comp

mv $project_dir/scripts/simple_nn/SIMPLE-NN/examples/SiO2/ab_initio_output/OUTCAR_comp \
    scripts/simple_nn/OUTCAR

Change DATASET_FP to reflect location of parent folder
Change database name as appropriate

Run: $ python3 <script_name>.py -i (or --ip) <database_ip>

Properties
----------
potential energy
forces

Other properties added to metadata
----------------------------------
None

File notes
----------
The OUTCAR_comp file is renamed OUTCAR so some of the included ASE utilities
function correctly. The file does not have entirely the expected format,
so I have borrowed a large chunk of ASE code and altered the line corresponding
to the break between images, or training frames.

Energy is obtained using get_potential_energy, but these are the same
values as contained in the OUTCAR file.

"""
from ase.utils import reader
from ase.io.utils import ImageIterator
from argparse import ArgumentParser
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
import numpy as np
import re
import sys

# Imports from borrowed ASE code
from abc import ABC, abstractmethod
from typing import Dict, Any, Sequence, TextIO, Iterator, Optional, Union, List
from warnings import warn
from pathlib import Path, PurePath

import ase
from ase import Atoms
from ase.data import atomic_numbers
from ase.io import ParseError, read
from ase.io.utils import ImageChunk
from ase.calculators.singlepoint import (
    SinglePointDFTCalculator,
    SinglePointKPoint,
)

DATASET_FP = Path().cwd()
GLOB_STR = "OUTCAR*"

DATASET = "SIMPLE_NN_SiO2"

SOFTWARE = "VASP"
METHODS = "DFT(PBE-GGA)"
LINKS = [
    "https://doi.org/10.17632/pjv2yr7pvr.1",
    "https://doi.org/10.1016/j.cpc.2019.04.014",
]
AUTHORS = "Kyuhyun Lee", "Dongsun Yoo", "Wonseok Jeong", "Seungwu Han"
DS_DESC = "10,000 configurations of SiO2 used as an example for the\
 SIMPLE-NN machine learning model. Dataset includes three types of crystals:\
 quartz, cristobalite and tridymite; amorphous; and liquid phase SiO2.\
 Structures with distortion from compression, monoaxial strain and shear\
 strain were also included in the training set."
ELEMENTS = ["O", "Si"]


def outcar_reader(filepath):
    data = read_vasp_out(filepath, index=":")
    configs = []
    for i, dat in enumerate(data):
        config = AtomicConfiguration(
            numbers=dat.numbers, positions=dat.positions, cell=dat.cell
        )
        config.info["energy"] = dat.get_potential_energy()
        config.info["force"] = dat.get_forces()
        config.info["name"] = f"SIMPLE_NN_SiO2_{i}"
        configs.append(config)

    return configs


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    args = parser.parse_args(argv)
    client = MongoDatabase("----", nprocs=4, uri=f"mongodb://{args.ip}:27017")

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=outcar_reader,
        glob_string=GLOB_STR,
        generator=False,
    )
    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)

    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        # "": {"field": "", "units": ""}
    }
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
                "forces": {"field": "force", "units": "eV/A"},
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
    # cs_regexes = [
    #     [
    #         DATASET,
    #         ".*",
    #         f"All configurations from {DATASET} dataset",
    #     ]
    # ]

    # cs_ids = []

    # for i, (name, regex, desc) in enumerate(cs_regexes):
    #     co_ids = client.get_data(
    #         "configurations",
    #         fields="hash",
    #         query={
    #             "hash": {"$in": all_co_ids},
    #             "names": {"$regex": regex},
    #         },
    #         ravel=True,
    #     ).tolist()

    #     print(
    #         f"Configuration set {i}",
    #         f"({name}):".rjust(22),
    #         f"{len(co_ids)}".rjust(7),
    #     )
    #     if len(co_ids) > 0:
    #         cs_id = client.insert_configuration_set(
    #             co_ids, description=desc, name=name
    #         )

    #         cs_ids.append(cs_id)
    #     else:
    #         pass

    client.insert_dataset(
        pr_hashes=all_do_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


def get_atomtypes(fname):
    """Given a file name, get the atomic symbols.

    The function can get this information from OUTCAR and POTCAR
    format files.  The files can also be compressed with gzip or
    bzip2.

    """
    fpath = Path(fname)

    atomtypes = []
    atomtypes_alt = []
    if fpath.suffix == ".gz":
        import gzip

        opener = gzip.open
    elif fpath.suffix == ".bz2":
        import bz2

        opener = bz2.BZ2File
    else:
        opener = open
    with opener(fpath) as fd:
        for line in fd:
            if "TITEL" in line:
                atomtypes.append(line.split()[3].split("_")[0].split(".")[0])
            elif "POTCAR:" in line:
                atomtypes_alt.append(
                    line.split()[2].split("_")[0].split(".")[0]
                )

    if len(atomtypes) == 0 and len(atomtypes_alt) > 0:
        # old VASP doesn't echo TITEL, but all versions print out species lines
        # preceded by "POTCAR:", twice
        if len(atomtypes_alt) % 2 != 0:
            raise ParseError(
                f'Tried to get atom types from {len(atomtypes_alt)} "POTCAR": '
                "lines in OUTCAR, but expected an even number"
            )
        atomtypes = atomtypes_alt[0 : len(atomtypes_alt) // 2]

    return atomtypes


def atomtypes_outpot(posfname, numsyms):
    """Try to retrieve chemical symbols from OUTCAR or POTCAR

    If getting atomtypes from the first line in POSCAR/CONTCAR fails, it might
    be possible to find the data in OUTCAR or POTCAR, if these files exist.

    posfname -- The filename of the POSCAR/CONTCAR file we're trying to read

    numsyms -- The number of symbols we must find

    """
    posfpath = Path(posfname)

    # Check files with exactly same path except POTCAR/OUTCAR instead
    # of POSCAR/CONTCAR.
    fnames = [posfpath.with_name("POTCAR"), posfpath.with_name("OUTCAR")]
    # Try the same but with compressed files
    fsc = []
    for fnpath in fnames:
        fsc.append(fnpath.parent / (fnpath.name + ".gz"))
        fsc.append(fnpath.parent / (fnpath.name + ".bz2"))
    for f in fsc:
        fnames.append(f)
    # Code used to try anything with POTCAR or OUTCAR in the name
    # but this is no longer supported

    tried = []
    for fn in fnames:
        if fn in posfpath.parent.iterdir():
            tried.append(fn)
            at = get_atomtypes(fn)
            if len(at) == numsyms:
                return at

    raise ParseError(
        "Could not determine chemical symbols. Tried files " + str(tried)
    )


def iread_vasp_out(filename, index=-1):
    """Import OUTCAR type file, as a generator."""
    it = ImageIterator(outcarchunks)
    return it(filename, index=index)


@reader
def read_vasp_out(filename="OUTCAR", index=-1):
    """Import OUTCAR type file.

    Reads unitcell, atom positions, energies, and forces from the OUTCAR file
    and attempts to read constraints (if any) from CONTCAR/POSCAR, if present.
    """
    # "filename" is actually a file-descriptor thanks to @reader
    g = iread_vasp_out(filename, index=index)
    # Code borrowed from formats.py:read
    if isinstance(index, (slice, str)):
        # Return list of atoms
        return list(g)
    else:
        # Return single atoms object
        return next(g)


"""
Module for parsing OUTCAR files.
"""

# Denotes end of Ionic step for OUTCAR reading
_OUTCAR_SCF_DELIM = "FREE ENERGIE OF THE ION-ELECTRON SYSTEM"

# Some type aliases
_HEADER = Dict[str, Any]
_CURSOR = int
_CHUNK = Sequence[str]
_RESULT = Dict[str, Any]


class NoNonEmptyLines(Exception):
    """No more non-empty lines were left in the provided chunck"""


class UnableToLocateDelimiter(Exception):
    """Did not find the provided delimiter"""

    def __init__(self, delimiter, msg):
        self.delimiter = delimiter
        super().__init__(msg)


def _check_line(line: str) -> str:
    """Auxiliary check line function for OUTCAR numeric formatting.
    See issue #179, https://gitlab.com/ase/ase/issues/179
    Only call in cases we need the numeric values
    """
    if re.search("[0-9]-[0-9]", line):
        line = re.sub("([0-9])-([0-9])", r"\1 -\2", line)
    return line


def find_next_non_empty_line(cursor: _CURSOR, lines: _CHUNK) -> _CURSOR:
    """Fast-forward the cursor from the current position to the next
    line which is non-empty.
    Returns the new cursor position on the next non-empty line.
    """
    for line in lines[cursor:]:
        if line.strip():
            # Line was non-empty
            return cursor
        # Empty line, increment the cursor position
        cursor += 1
    # There was no non-empty line
    raise NoNonEmptyLines("Did not find a next line which was not empty")


def search_lines(delim: str, cursor: _CURSOR, lines: _CHUNK) -> _CURSOR:
    """Search through a chunk of lines starting at the cursor position for
    a given delimiter. The new position of the cursor is returned."""
    for line in lines[cursor:]:
        if delim in line:
            # The cursor should be on the line with the delimiter now
            assert delim in lines[cursor]
            return cursor
        # We didn't find the delimiter
        cursor += 1
    raise UnableToLocateDelimiter(
        delim, f"Did not find starting point for delimiter {delim}"
    )


def convert_vasp_outcar_stress(stress: Sequence):
    """Helper function to convert the stress line in an OUTCAR to the
    expected units in ASE"""
    stress_arr = -np.array(stress)
    shape = stress_arr.shape
    if shape != (6,):
        raise ValueError(
            "Stress has the wrong shape. Expected (6,), got {}".format(shape)
        )
    stress_arr = stress_arr[[0, 1, 2, 4, 5, 3]] * 1e-1 * ase.units.GPa
    return stress_arr


def read_constraints_from_file(directory):
    directory = Path(directory)
    constraint = None
    for filename in ("CONTCAR", "POSCAR"):
        if (directory / filename).is_file():
            constraint = read(directory / filename, format="vasp").constraints
            break
    return constraint


class VaspPropertyParser(ABC):
    NAME = None  # type: str

    @classmethod
    def get_name(cls):
        """Name of parser. Override the NAME constant in the class to
        specify a custom name,
        otherwise the class name is used"""
        return cls.NAME or cls.__name__

    @abstractmethod
    def has_property(self, cursor: _CURSOR, lines: _CHUNK) -> bool:
        """Function which checks if a property can be derived from a given
        cursor position"""

    @staticmethod
    def get_line(cursor: _CURSOR, lines: _CHUNK) -> str:
        """Helper function to get a line, and apply the check_line function"""
        return _check_line(lines[cursor])

    @abstractmethod
    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        """Extract a property from the cursor position.
        Assumes that "has_property" would evaluate to True
        from cursor position"""


class SimpleProperty(VaspPropertyParser, ABC):
    LINE_DELIMITER = None  # type: str

    def __init__(self):
        super().__init__()
        if self.LINE_DELIMITER is None:
            raise ValueError("Must specify a line delimiter.")

    def has_property(self, cursor, lines) -> bool:
        line = lines[cursor]
        return self.LINE_DELIMITER in line


class VaspChunkPropertyParser(VaspPropertyParser, ABC):
    """Base class for parsing a chunk of the OUTCAR.
    The base assumption is that only a chunk of lines is passed"""

    def __init__(self, header: _HEADER = None):
        super().__init__()
        header = header or {}
        self.header = header

    def get_from_header(self, key: str) -> Any:
        """Get a key from the header, and raise a ParseError
        if that key doesn't exist"""
        try:
            return self.header[key]
        except KeyError:
            raise ParseError(
                'Parser requested unavailable key "{}" from header'.format(key)
            )


class VaspHeaderPropertyParser(VaspPropertyParser, ABC):
    """Base class for parsing the header of an OUTCAR"""


class SimpleVaspChunkParser(VaspChunkPropertyParser, SimpleProperty, ABC):
    """Class for properties in a chunk can be
    determined to exist from 1 line"""


class SimpleVaspHeaderParser(VaspHeaderPropertyParser, SimpleProperty, ABC):
    """Class for properties in the header
    which can be determined to exist from 1 line"""


class Spinpol(SimpleVaspHeaderParser):
    """Parse if the calculation is spin-polarized.
    Example line:
    "   ISPIN  =      2    spin polarized calculation?"
    """

    LINE_DELIMITER = "ISPIN"

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = lines[cursor].strip()
        parts = line.split()
        ispin = int(parts[2])
        # ISPIN 2 = spinpolarized, otherwise no
        # ISPIN 1 = non-spinpolarized
        spinpol = ispin == 2
        return {"spinpol": spinpol}


class SpeciesTypes(SimpleVaspHeaderParser):
    """Parse species types.
    Example line:
    " POTCAR:    PAW_PBE Ni 02Aug2007"
    We must parse this multiple times, as it's scattered in the header.
    So this class has to simply parse the entire header.
    """

    LINE_DELIMITER = "POTCAR:"

    def __init__(self, *args, **kwargs):
        self._species = []  # Store species as we find them
        # We count the number of times we found the line,
        # as we only want to parse every second,
        # due to repeated entries in the OUTCAR
        super().__init__(*args, **kwargs)

    @property
    def species(self) -> List[str]:
        """Internal storage of each found line.
        Will contain the double counting.
        Use the get_species() method to get the un-doubled list."""
        return self._species

    def get_species(self) -> List[str]:
        """The OUTCAR will contain two 'POTCAR:' entries per species.
        This method only returns the first half,
        effectively removing the double counting.
        """
        # Get the index of the first half
        # In case we have an odd number, we round up (for testing purposes)
        # Tests like to just add species 1-by-1
        # Having an odd number should never happen in a real OUTCAR
        # For even length lists, this is just equivalent to idx =
        # len(self.species) // 2
        idx = sum(divmod(len(self.species), 2))
        # Make a copy
        return list(self.species[:idx])

    def _make_returnval(self) -> _RESULT:
        """Construct the return value for the "parse" method"""
        return {"species": self.get_species()}

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = lines[cursor].strip()

        parts = line.split()
        # Determine in what position we'd expect to find the symbol
        if "1/r potential" in line:
            # This denotes an AE potential
            # Currently only H_AE
            # "  H  1/r potential  "
            idx = 1
        else:
            # Regular PAW potential, e.g.
            # "PAW_PBE H1.25 07Sep2000" or
            # "PAW_PBE Fe_pv 02Aug2007"
            idx = 2

        sym = parts[idx]
        # remove "_h", "_GW", "_3" tags etc.
        sym = sym.split("_")[0]
        # in the case of the "H1.25" potentials etc.,
        # remove any non-alphabetic characters
        sym = "".join([s for s in sym if s.isalpha()])

        if sym not in atomic_numbers:
            # Check that we have properly parsed the symbol, and we found
            # an element
            raise ParseError(
                f"Found an unexpected symbol {sym} in line {line}"
            )

        self.species.append(sym)

        return self._make_returnval()


class IonsPerSpecies(SimpleVaspHeaderParser):
    """Example line:
    "   ions per type =              32  31   2"
    """

    LINE_DELIMITER = "ions per type"

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = lines[cursor].strip()
        parts = line.split()
        ion_types = list(map(int, parts[4:]))
        return {"ion_types": ion_types}


class KpointHeader(VaspHeaderPropertyParser):
    """Reads nkpts and nbands from the line delimiter.
    Then it also searches for the ibzkpts and kpt_weights"""

    def has_property(self, cursor: _CURSOR, lines: _CHUNK) -> bool:
        line = lines[cursor]
        return "NKPTS" in line and "NBANDS" in line

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = lines[cursor].strip()
        parts = line.split()
        nkpts = int(parts[3])
        nbands = int(parts[-1])

        results: Dict[str, Any] = {"nkpts": nkpts, "nbands": nbands}
        # We also now get the k-point weights etc.,
        # because we need to know how many k-points we have
        # for parsing that
        # Move cursor down to next delimiter
        delim2 = "k-points in reciprocal lattice and weights"
        for offset, line in enumerate(lines[cursor:], start=0):
            line = line.strip()
            if delim2 in line:
                # build k-points
                ibzkpts = np.zeros((nkpts, 3))
                kpt_weights = np.zeros(nkpts)
                for nk in range(nkpts):
                    # Offset by 1, as k-points starts on the next line
                    line = lines[cursor + offset + nk + 1].strip()
                    parts = line.split()
                    ibzkpts[nk] = list(map(float, parts[:3]))
                    kpt_weights[nk] = float(parts[-1])
                results["ibzkpts"] = ibzkpts
                results["kpt_weights"] = kpt_weights
                break
        else:
            raise ParseError("Did not find the K-points in the OUTCAR")

        return results


class Stress(SimpleVaspChunkParser):
    """Process the stress from an OUTCAR"""

    LINE_DELIMITER = "in kB "

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = self.get_line(cursor, lines)
        result = None  # type: Optional[Sequence[float]]
        try:
            stress = [float(a) for a in line.split()[2:]]
        except ValueError:
            # Vasp FORTRAN string formatting issues, can happen with
            # some bad geometry steps Alternatively, we can re-raise
            # as a ParseError?
            warn("Found badly formatted stress line. Setting stress to None.")
        else:
            result = convert_vasp_outcar_stress(stress)
        return {"stress": result}


class Cell(SimpleVaspChunkParser):
    LINE_DELIMITER = "direct lattice vectors"

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        nskip = 1
        cell = np.zeros((3, 3))
        for i in range(3):
            line = self.get_line(cursor + i + nskip, lines)
            parts = line.split()
            cell[i, :] = list(map(float, parts[0:3]))
        return {"cell": cell}


class PositionsAndForces(SimpleVaspChunkParser):
    """Positions and forces are written in the same block.
    We parse both simultaneously"""

    LINE_DELIMITER = "POSITION          "

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        nskip = 2
        natoms = self.get_from_header("natoms")
        positions = np.zeros((natoms, 3))
        forces = np.zeros((natoms, 3))

        for i in range(natoms):
            line = self.get_line(cursor + i + nskip, lines)
            parts = list(map(float, line.split()))
            positions[i] = parts[0:3]
            forces[i] = parts[3:6]
        return {"positions": positions, "forces": forces}


class Magmom(VaspChunkPropertyParser):
    def has_property(self, cursor: _CURSOR, lines: _CHUNK) -> bool:
        """We need to check for two separate delimiter strings,
        to ensure we are at the right place"""
        line = lines[cursor]
        if "number of electron" in line:
            parts = line.split()
            if len(parts) > 5 and parts[0].strip() != "NELECT":
                return True
        return False

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = self.get_line(cursor, lines)
        parts = line.split()
        idx = parts.index("magnetization") + 1
        magmom_lst = parts[idx:]
        if len(magmom_lst) != 1:
            warn(
                "Non-collinear spin is not yet implemented. "
                "Setting magmom to x value."
            )
        magmom = float(magmom_lst[0])
        # Use these lines when non-collinear spin is supported!
        # Remember to check that format fits!
        # else:
        #     # Non-collinear spin
        #     # Make a (3,) dim array
        #     magmom = np.array(list(map(float, magmom)))
        return {"magmom": magmom}


class Magmoms(SimpleVaspChunkParser):
    """Get the x-component of the magnitization.
    This is just the magmoms in the collinear case.
    non-collinear spin is (currently) not supported"""

    LINE_DELIMITER = "magnetization (x)"

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        # Magnetization for collinear
        natoms = self.get_from_header("natoms")
        nskip = 4  # Skip some lines
        magmoms = np.zeros(natoms)
        for i in range(natoms):
            line = self.get_line(cursor + i + nskip, lines)
            magmoms[i] = float(line.split()[-1])
        # Once we support non-collinear spin,
        # search for magnetization (y) and magnetization (z) as well.
        return {"magmoms": magmoms}


class EFermi(SimpleVaspChunkParser):
    LINE_DELIMITER = "E-fermi :"

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        line = self.get_line(cursor, lines)
        parts = line.split()
        efermi = float(parts[2])
        return {"efermi": efermi}


class Energy(SimpleVaspChunkParser):
    LINE_DELIMITER = _OUTCAR_SCF_DELIM

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        nskip = 2
        line = self.get_line(cursor + nskip, lines)
        parts = line.strip().split()
        energy_free = float(parts[4])  # Force consistent

        nskip = 4
        line = self.get_line(cursor + nskip, lines)
        parts = line.strip().split()
        energy_zero = float(parts[6])  # Extrapolated to 0 K

        return {"free_energy": energy_free, "energy": energy_zero}


class Kpoints(VaspChunkPropertyParser):
    def has_property(self, cursor: _CURSOR, lines: _CHUNK) -> bool:
        line = lines[cursor]
        # Example line:
        # " spin component 1" or " spin component 2"
        # We only check spin up, as if we are spin-polarized, we'll parse that
        # as well
        if "spin component 1" in line:
            parts = line.strip().split()
            # This string is repeated elsewhere, but not with this exact shape
            if len(parts) == 3:
                try:
                    # The last part of te line should be an integer, denoting
                    # spin-up or spin-down
                    int(parts[-1])
                except ValueError:
                    pass
                else:
                    return True
        return False

    def parse(self, cursor: _CURSOR, lines: _CHUNK) -> _RESULT:
        nkpts = self.get_from_header("nkpts")
        nbands = self.get_from_header("nbands")
        weights = self.get_from_header("kpt_weights")
        spinpol = self.get_from_header("spinpol")
        nspins = 2 if spinpol else 1

        kpts = []
        for spin in range(nspins):
            # for Vasp 6, they added some extra information after the
            # spin components.  so we might need to seek the spin
            # component line
            cursor = search_lines(f"spin component {spin + 1}", cursor, lines)

            cursor += 2  # Skip two lines
            for _ in range(nkpts):
                # Skip empty lines
                cursor = find_next_non_empty_line(cursor, lines)

                line = self.get_line(cursor, lines)
                # Example line:
                # "k-point     1 :       0.0000    0.0000    0.0000"
                parts = line.strip().split()
                ikpt = int(parts[1]) - 1  # Make kpt idx start from 0
                weight = weights[ikpt]

                cursor += 2  # Move down two
                eigenvalues = np.zeros(nbands)
                occupations = np.zeros(nbands)
                for n in range(nbands):
                    # Example line:
                    # "      1      -9.9948      1.00000"
                    parts = lines[cursor].strip().split()
                    eps_n, f_n = map(float, parts[1:])
                    occupations[n] = f_n
                    eigenvalues[n] = eps_n
                    cursor += 1
                kpt = SinglePointKPoint(
                    weight, spin, ikpt, eps_n=eigenvalues, f_n=occupations
                )
                kpts.append(kpt)

        return {"kpts": kpts}


class DefaultParsersContainer:
    """Container for the default OUTCAR parsers.
    Allows for modification of the global default parsers.
    Takes in an arbitrary number of parsers.
    The parsers should be uninitialized,
    as they are created on request.
    """

    def __init__(self, *parsers_cls):
        self._parsers_dct = {}
        for parser in parsers_cls:
            self.add_parser(parser)

    @property
    def parsers_dct(self) -> dict:
        return self._parsers_dct

    def make_parsers(self):
        """Return a copy of the internally stored parsers.
        Parsers are created upon request."""
        return list(parser() for parser in self.parsers_dct.values())

    def remove_parser(self, name: str):
        """Remove a parser based on the name.
        The name must match the parser name exactly."""
        self.parsers_dct.pop(name)

    def add_parser(self, parser) -> None:
        """Add a parser"""
        self.parsers_dct[parser.get_name()] = parser


class TypeParser(ABC):
    """Base class for parsing a type, e.g. header or chunk,
    by applying the internal attached parsers"""

    def __init__(self, parsers):
        self.parsers = parsers

    @property
    def parsers(self):
        return self._parsers

    @parsers.setter
    def parsers(self, new_parsers) -> None:
        self._check_parsers(new_parsers)
        self._parsers = new_parsers

    @abstractmethod
    def _check_parsers(self, parsers) -> None:
        """Check the parsers are of correct type"""

    def parse(self, lines) -> _RESULT:
        """Execute the attached paresers, and return the parsed properties"""
        properties = {}
        for cursor, _ in enumerate(lines):
            for parser in self.parsers:
                # Check if any of the parsers can extract a property
                # from this line Note: This will override any existing
                # properties we found, if we found it previously. This
                # is usually correct, as some VASP settings can cause
                # certain pieces of information to be written multiple
                # times during SCF. We are only interested in the
                # final values within a given chunk.
                if parser.has_property(cursor, lines):
                    prop = parser.parse(cursor, lines)
                    properties.update(prop)
        return properties


class ChunkParser(TypeParser, ABC):
    def __init__(self, parsers, header=None):
        super().__init__(parsers)
        self.header = header

    @property
    def header(self) -> _HEADER:
        return self._header

    @header.setter
    def header(self, value: Optional[_HEADER]) -> None:
        self._header = value or {}
        self.update_parser_headers()

    def update_parser_headers(self) -> None:
        """Apply the header to all available parsers"""
        for parser in self.parsers:
            parser.header = self.header

    def _check_parsers(
        self, parsers: Sequence[VaspChunkPropertyParser]
    ) -> None:
        """Check the parsers are of correct type 'VaspChunkPropertyParser'"""
        if not all(
            isinstance(parser, VaspChunkPropertyParser) for parser in parsers
        ):
            raise TypeError(
                "All parsers must be of type VaspChunkPropertyParser"
            )

    @abstractmethod
    def build(self, lines: _CHUNK) -> Atoms:
        """Construct an atoms object of the chunk from the parsed results"""


class HeaderParser(TypeParser, ABC):
    def _check_parsers(
        self, parsers: Sequence[VaspHeaderPropertyParser]
    ) -> None:
        """Check the parsers are of correct type 'VaspHeaderPropertyParser'"""
        if not all(
            isinstance(parser, VaspHeaderPropertyParser) for parser in parsers
        ):
            raise TypeError(
                "All parsers must be of type VaspHeaderPropertyParser"
            )

    @abstractmethod
    def build(self, lines: _CHUNK) -> _HEADER:
        """Construct the header object from the parsed results"""


class OutcarChunkParser(ChunkParser):
    """Class for parsing a chunk of an OUTCAR."""

    def __init__(
        self,
        header: _HEADER = None,
        parsers: Sequence[VaspChunkPropertyParser] = None,
    ):
        global default_chunk_parsers
        parsers = parsers or default_chunk_parsers.make_parsers()
        super().__init__(parsers, header=header)

    def build(self, lines: _CHUNK) -> Atoms:
        """Apply outcar chunk parsers, and build an atoms object"""
        self.update_parser_headers()  # Ensure header is in sync

        results = self.parse(lines)
        symbols = self.header["symbols"]
        constraint = self.header.get("constraint", None)

        atoms_kwargs = dict(symbols=symbols, constraint=constraint, pbc=True)

        # Find some required properties in the parsed results.
        # Raise ParseError if they are not present
        for prop in ("positions", "cell"):
            try:
                atoms_kwargs[prop] = results.pop(prop)
            except KeyError:
                raise ParseError(
                    "Did not find required property {} during parse.".format(
                        prop
                    )
                )
        atoms = Atoms(**atoms_kwargs)

        kpts = results.pop("kpts", None)
        calc = SinglePointDFTCalculator(atoms, **results)
        if kpts is not None:
            calc.kpts = kpts
        calc.name = "vasp"
        atoms.calc = calc
        return atoms


class OutcarHeaderParser(HeaderParser):
    """Class for parsing a chunk of an OUTCAR."""

    def __init__(
        self,
        parsers: Sequence[VaspHeaderPropertyParser] = None,
        workdir: Union[str, PurePath] = None,
    ):
        global default_header_parsers
        parsers = parsers or default_header_parsers.make_parsers()
        super().__init__(parsers)
        self.workdir = workdir

    @property
    def workdir(self):
        return self._workdir

    @workdir.setter
    def workdir(self, value):
        if value is not None:
            value = Path(value)
        self._workdir = value

    def _build_symbols(self, results: _RESULT) -> Sequence[str]:
        if "symbols" in results:
            # Safeguard, in case a different parser already
            # did this. Not currently available in a default parser
            return results.pop("symbols")

        # Build the symbols of the atoms
        for required_key in ("ion_types", "species"):
            if required_key not in results:
                raise ParseError(
                    'Did not find required key "{}" in parsed header results.'.format(
                        required_key
                    )
                )

        ion_types = results.pop("ion_types")
        species = results.pop("species")
        if len(ion_types) != len(species):
            raise ParseError(
                (
                    "Expected length of ion_types to be same as species, "
                    "but got ion_types={} and species={}"
                ).format(len(ion_types), len(species))
            )

        # Expand the symbols list
        symbols = []
        for n, sym in zip(ion_types, species):
            symbols.extend(n * [sym])
        return symbols

    def _get_constraint(self):
        """Try and get the constraints from the POSCAR of CONTCAR
        since they aren't located in the OUTCAR, and thus we cannot construct an
        OUTCAR parser which does this.
        """
        constraint = None
        if self.workdir is not None:
            constraint = read_constraints_from_file(self.workdir)
        return constraint

    def build(self, lines: _CHUNK) -> _RESULT:
        """Apply the header parsers, and build the header"""
        results = self.parse(lines)

        # Get the symbols from the parsed results
        # will pop the keys which we use for that purpose
        symbols = self._build_symbols(results)
        natoms = len(symbols)

        constraint = self._get_constraint()

        # Remaining results from the parse goes into the header
        header = dict(
            symbols=symbols, natoms=natoms, constraint=constraint, **results
        )
        return header


class OUTCARChunk(ImageChunk):
    """Container class for a chunk of the OUTCAR which consists of a
    self-contained SCF step, i.e. and image. Also contains the header_data
    """

    def __init__(
        self, lines: _CHUNK, header: _HEADER, parser: ChunkParser = None
    ):
        super().__init__()
        self.lines = lines
        self.header = header
        self.parser = parser or OutcarChunkParser()

    def build(self):
        self.parser.header = self.header  # Ensure header is syncronized
        return self.parser.build(self.lines)


def build_header(fd: TextIO) -> _CHUNK:
    """Build a chunk containing the header data"""
    lines = []
    for line in fd:
        lines.append(line)
        if (
            "-0.384840785 -0.549448925  9.915043234     0.003774966  0.005548756  0.101310856"
            in line
        ):
            # Start of SCF cycle
            return lines

    # We never found the SCF delimiter, so the OUTCAR must be incomplete
    raise ParseError("Incomplete OUTCAR")


def build_chunk(fd: TextIO) -> _CHUNK:
    """Build chunk which contains 1 complete atoms object"""
    lines = []
    while True:
        line = next(fd)
        lines.append(line)
        if _OUTCAR_SCF_DELIM in line:
            # Add 4 more lines to include energy
            for _ in range(4):
                lines.append(next(fd))
            break
    return lines


def outcarchunks(
    fd: TextIO,
    chunk_parser: ChunkParser = None,
    header_parser: HeaderParser = None,
) -> Iterator[OUTCARChunk]:
    """Function to build chunks of OUTCAR from a file stream"""
    name = Path(fd.name)
    workdir = name.parent

    # First we get header info
    # pass in the workdir from the fd, so we can try and get the constraints
    header_parser = header_parser or OutcarHeaderParser(workdir=workdir)

    lines = build_header(fd)
    header = header_parser.build(lines)
    assert isinstance(header, dict)

    chunk_parser = chunk_parser or OutcarChunkParser()

    while True:
        try:
            lines = build_chunk(fd)
        except StopIteration:
            # End of file
            return
        yield OUTCARChunk(lines, header, parser=chunk_parser)


# Create the default chunk parsers
default_chunk_parsers = DefaultParsersContainer(
    Cell,
    PositionsAndForces,
    Stress,
    Magmoms,
    Magmom,
    EFermi,
    Kpoints,
    Energy,
)

# Create the default header parsers
default_header_parsers = DefaultParsersContainer(
    SpeciesTypes,
    IonsPerSpecies,
    Spinpol,
    KpointHeader,
)
# Footer
# © 2023 GitHub, Inc.
# Footer navigation

#     Terms
#     Privacy
#     Security
#     Status
#     Docs
#     Contact GitHub
#     Pricing
#     API
#     Training
#     Blog
#     About

# ase/vasp_outcar_parsers.py at 111947e23373218d739252a28fb6284e1e985ee7 · rosswhitfield/ase


if __name__ == "__main__":
    main(sys.argv[1:])

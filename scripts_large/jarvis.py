"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties
spg = space group
fund = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
optb88vdw_total_energy (dft_3d)
"""
from argparse import ArgumentParser
import json
from pathlib import Path
import sys

from colabfit.tools.configuration import AtomicConfiguration

DATASET_FP = Path("scripts_large/jarvis_json_zips/")
GLOB = "jdft_3d-12-12-2022.json"
DS_NAME = "JARVIS-DFT-3D-12-12-2022"
DS_DESC = ""

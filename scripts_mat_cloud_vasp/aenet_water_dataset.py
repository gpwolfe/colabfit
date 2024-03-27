"""
author: Gregory Wolfe

Properties
----------

Other properties added to metadata
----------------------------------

File notes
----------

"""

import functools
import logging
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

import pymongo
from ase.io import read

# from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import MongoDatabase, generate_ds_id, load_data
from colabfit.tools.property_definitions import (  # cauchy_stress_pd,
    atomic_forces_pd,
    potential_energy_pd,
)

DATASET_FP = Path(
    "data/aenet-lammps_and_aenet-tinker:_interfaces_for_accurate_and_efficient_"
    "molecular_dynamics_simulations_with_machine_learning_potentials/"
    "liquid-water-revPBE-D3"
)
DATASET_NAME = "AENET_liquid_water_dataset_JCP2021"
LICENSE = "CC-BY-4.0"
PUB_YEAR = "2020"

PUBLICATION = "http://doi.org/10.1063/5.0063880"
DATA_LINK = "https://doi.org/10.24435/materialscloud:dx-ct"
# OTHER_LINKS = []

AUTHORS = [
    "Michael S. Chen",
    "Tobias Morawietz",
    "Thomas E. Markland",
    "Nongnuch Artrith",
]
DATASET_DESC = (
    "The water data set comprises energies and forces of 9,189 "
    "condensed-phase structures. The data was obtained in an iterative "
    "procedure described in detail in Ref. [4]. The final ANN potential was "
    "employed in Refs. [4,5] to analyze temperature-dependent Raman spectra "
    "of liquid water. "
    "The data set contains structures from four iterations: Initial "
    "structures (iteration 0) were obtained from classical and path integral "
    "AIMD simulations of bulk liquid water in a cubic box containing 64 water "
    "molecules at 300 K as reported in Ref. [6]. Distorted configurations "
    "with higher forces were added by randomly displacing the Cartesian "
    "coordinates of these configurations. Iteration 1 contains a set of 500 "
    "configurations from MD simulations with the fully flexible SPC/E flex "
    "water model [7] employing a 25 % increased water density (simulation box "
    "with 80 water molecules) and elevated temperatures (T = 500 K) in order "
    "to sample highly repulsive configurations. Structures in iteration 2 "
    "were obtained by classical MD simulations with preliminary ANN "
    "potentials at T = 300 K, 325 K, 350 K, and 370 K employing cubic boxes "
    "with 64 molecules and the corresponding experimental densities. The "
    "final iteration 3 data contains structures from preliminary ANN "
    "simulations with classical and quantum nuclei, respectively, at a wide "
    "range of temperatures (T = 258 K, 268 K, 280 K, 290 K, 300 K, 310 K, 320 "
    "K, 330 K, 340 K, 350 K, 360 K, and 370 K) using cubic boxes with 64 "
    "molecules and the corresponding experimental densities. "
    "Energies and atomic forces were calculated with the CP2K program [8,9] "
    "using the revPBE exchange-correlation functional [10,11] with D3 "
    "dispersion correction [12] following the setup reported in "
    "Ref. [4]. Atomic cores were represented using the dual-space "
    "Goedecker-Teter-Hutter pseudopotentials [13], Kohn-Sham orbitals were "
    "expanded in the TZV2P basis set within the GPW method [14], and the "
    "density was represented by an auxiliary plane-wave basis with a cutoff "
    "of 400 Ry. "
    "[1] A. Kokalj, J. Mol. Graphics Modell. 17, 176–179 (1999). "
    "[2] N. Artrith, A. Urban, Comput. Mater. Sci. 114, 135–150 (2016). "
    "[3] N. Artrith, A. Urban, G. Ceder, Phys. Rev. B 96, 014112 (2017). "
    "[4] T. Morawietz, O. Marsalek, S. R. Pattenaude, L. M. Streacker, D. Ben-Amotz, "
    "and T. E. Markland, J. Phys. Chem. Lett. 9, 851 (2018). "
    "[5] T. Morawietz, A. S. Urbina, P. K. Wise, X. Wu, W. Lu, D. Ben-Amotz, and T. E. "
    "Markland, J. Phys. Chem. Lett. 10, 6067 (2019). "
    "[6] Marsalek and T. E. Markland, J. Phys. Chem. Lett. 8, 1545 (2017). "
    "[7] X. B. Zhang, Q. L. Liu, and A. M. Zhu, Fluid Ph. Equilibria 262, 210(2007). "
    "[8] J. VandeVondele, M. Krack, F. Mohamed, M. Parrinello, T. Chassaing, and J. "
    "Hutter, Comput. Phys. Commun. 167, 103 (2005). "
    "[9] J. Hutter, M. Iannuzzi, F. Schiffmann, and J. VandeVondele, WIRES Comput. "
    "Mol. Sci. 4, 15 (2014). "
    "[10] J. P. Perdew, K. Burke, and M. Ernzerhof, Phys. Rev. Lett. 77, 3865 (1996). "
    "[11] Y. Zhang and W. Yang, Phys. Rev. Lett. 80, 890 (1998). "
    "[12] S. Grimme, J. Antony, S. Ehrlich, and H. Krieg, J. Chem. Phys. 132, 154104 "
    "(2010). "
    "[13] S. Goedecker, M. Teter, and J. Hutter, Phys. Rev. B 54, 1703 (1996). "
    "[14] B. G. Lippert, J. Hutter, and M. Parrinello, Mol. Phys. 92, 477 (1997). "
)
ELEMENTS = None
GLOB_STR = "*.xsf"

PI_METADATA = {
    "software": {"value": "CP2K"},
    "method": {"value": "DFT-revPBE-D3"},
    "basis-set": {"value": "TZV2P"},
    "input": {
        "value": {
            "ENCUT": "400 Ry",
        }
    },
}

PROPERTY_MAP = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "eV"},
            "per-atom": {"value": False, "units": None},
            "_metadata": PI_METADATA,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "eV/angstrom"},
            "_metadata": PI_METADATA,
        },
    ],
    # "cauchy-stress": [
    #     {
    #         "stress": {"field": "stress", "units": "kilobar"},
    #         "volume-normalized": {"value": False, "units": None},
    #         "_metadata": PI_METADATA,
    #     }
    # ],
}


CSS = [
    [
        f"{DATASET_NAME}_iteration_0",
        {"names": {"$regex": "iter0_"}},
        f"Structures from iteration 0 from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_iteration_1",
        {"names": {"$regex": "iter1_"}},
        f"Structures from iteration 1 from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_iteration_2",
        {"names": {"$regex": "iter2_"}},
        f"Structures from iteration 2 from {DATASET_NAME} dataset",
    ],
    [
        f"{DATASET_NAME}_iteration_3",
        {"names": {"$regex": "iter3_"}},
        f"Structures from iteration 3 from {DATASET_NAME} dataset",
    ],
]


def reader(filepath: Path):
    name = f"{DATASET_NAME}_{filepath.stem}"
    with filepath.open("r") as f:
        header = f.readline()
        energy = float(header.strip().split()[-1])
    config = read(filename=filepath, format="xsf")
    config.info["energy"] = energy
    config.info["name"] = name
    config.info["forces"] = config.get_forces()
    return [config]


MAX_AUTO_RECONNECT_ATTEMPTS = 100


def auto_reconnect(mongo_func):
    """Gracefully handle a reconnection event."""

    @functools.wraps(mongo_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_func(*args, **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt)  # exponential back off
                if wait_t > 1800:
                    wait_t = 1800  # cap at 1/2 hour
                logging.warning(
                    "PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",
                    str(e),
                    wait_t,
                )
                time.sleep(wait_t)

    return wrapper


@auto_reconnect
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
    parser.add_argument(
        "-r", "--port", type=int, help="Port to use for MongoDB client", default=27017
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:{args.port}"
    )

    client.insert_property_definition(atomic_forces_pd)
    client.insert_property_definition(potential_energy_pd)
    # client.insert_property_definition(cauchy_stress_pd)

    ds_id = generate_ds_id()

    configurations = load_data(
        file_path=DATASET_FP,
        file_format="folder",
        name_field="name",
        elements=ELEMENTS,
        reader=reader,
        glob_string=GLOB_STR,
        generator=False,
    )

    ids = list(
        client.insert_data(
            configurations=configurations,
            ds_id=ds_id,
            # co_md_map=CO_METADATA,
            property_map=PROPERTY_MAP,
            generator=False,
            verbose=True,
        )
    )

    all_co_ids, all_do_ids = list(zip(*ids))

    cs_ids = []
    for i, (name, query, desc) in enumerate(CSS):
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=query,
        )

        cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_do_ids,
        ds_id=ds_id,
        name=DATASET_NAME,
        authors=AUTHORS,
        links=[PUBLICATION, DATA_LINK],  # + OTHER_LINKS,
        description=DATASET_DESC,
        verbose=True,
        cs_ids=cs_ids,  # remove line if no configuration sets to insert
        data_license=LICENSE,
        publication_year=PUB_YEAR,
    )


if __name__ == "__main__":
    main(sys.argv[1:])

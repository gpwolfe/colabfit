"""
Get proper author names
check file names and configuration types/names
check for config-md
     Z    |      (n_atoms, )     |                   Atomic numbers of nuclei                   |                                                |  # noqa: E501
|    R    | (120000, n_atoms, 3) |                     Cartesian coordinates                    |                  Angstrom [A]                  |  # noqa: E501
|    E    |      (120000, 1)     |                       Potential energy                       |         kilocalories per mol [kcal/mol]        |  # noqa: E501
|    F    | (120000, n_atoms, 3) |                         Atomic forces                        | kilocalories per mol per Angstrom [kcal/mol/A] |  # noqa: E501
|    Q    | (120000, n_atoms, 1) |                       Mulliken charges                       |              elementary charge [e]             |  # noqa: E501
|    P    |      (120000, 6)     |                   Isotropic polarizability                   |               Bohr cubed [Bohr^3]              |  # noqa: E501
|    DP   |      (120000, 3)     |                     Dipole moment vectors                    |                   Debye [D]                    |  # noqa: E501
|    QP   |    (120000, 3, 3)    |                   Quadrupole moment matrix                   |               Debye-Angstrom [DA]              |  # noqa: E501
|    RC   |      (120000, 3)     |                     Rotational constants                     |                 gigahertz [GHz]                |  # noqa: E501
|    HL   |      (120000, 2)     |                    HOMO and LUMO energies                    |                electronvolt [eV]               |  # noqa: E501
|    R2   |      (120000, 1)     |                   Electronic spatial extent                  |              Bohr squared [Bohr^2]             |  # noqa: E501
|   CONF  |      (120000, 1)     |              String identifier of conformations              |                                                |  # noqa: E501


"""

from argparse import ArgumentParser
from ase import Atoms
import numpy as np
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import potential_energy_pd, atomic_forces_pd


# DS_NAME = "WS22"
AUTHORS = ["Max Pinheiro Jr", "Shuang Zhang", "Pavlo O. Dral", "Mario Barbatti"]

PUBLICATION = "https://doi.org/10.1038/s41597-023-01998-3"
DATA_LINK = "https://doi.org/10.5281/zenodo.7032333"
LINKS = [
    "https://doi.org/10.1038/s41597-023-01998-3",
    "https://doi.org/10.5281/zenodo.7032333",
]
DATASET_FP = Path("/persistent/colabfit_raw_data/new_raw_datasets_2.0/WS22_database")
DATASET_FP = Path().cwd().parent / ("data/ws22")  # local
DS_DESC = (
    "The WS22 database combines Wigner sampling with geometry interpolation to generate"
    " 1.18 million molecular geometries equally distributed into 10 independent "
    "datasets of flexible organic molecules with varying sizes and chemical "
    "complexity. In addition to the potential energy and forces required to construct "
    "potential energy surfaces, the WS22 database provides several other quantum "
    "chemical properties, all obtained via single-point calculations for each "
    "molecular geometry. All quantum chemical calculations were performed with the "
    "Gaussian 09 program."
)


def reader_ws22(p):
    atoms = []
    a = np.load(p)

    z = a["Z"]  # atomic numbers
    r = a["R"]  # coordinates
    e = a["E"]  # potential energy
    f = a["F"]  # forces
    mulliken = a["Q"]  # mulliken charges
    iso_pol = a["P"]  # isotropic polarizability
    dip_mom = a["DP"]  # dipole-moment vectors
    quad_mom = a["QP"]  # quadrupole moment matrix
    rot_const = a["RC"]  # rotational constants
    he = [x[0] for x in a["HL"]]  # homo-energy
    le = [x[1] for x in a["HL"]]  # lumo-energy
    elec_extent = a["R2"]  # electronic spatial extent
    conf = a["CONF"]  # conformation identifier string

    # q=a['nuclear_charges']
    for i in range(r.shape[0]):
        # for i in range(2000):  # for local testing purposes
        atom = Atoms(numbers=z, positions=r[i])
        atom.info["name"] = f"{p.stem}_i"
        atom.info["energy"] = float(e[i])
        atom.arrays["forces"] = f[i]

        atom.info["mulliken"] = mulliken[i]
        atom.info["iso_pol"] = iso_pol[i]
        atom.info["dip_mom"] = dip_mom[i]
        atom.info["quad_mom"] = quad_mom[i]
        atom.info["rot_const"] = rot_const[i]
        atom.info["homo_energy"] = he[i]
        atom.info["lumo_energy"] = le[i]
        atom.info["elec_extent"] = elec_extent[i]
        atom.info["conf"] = conf[i]

        atoms.append(atom)
    return atoms


co_md = {
    "mulliken-charges": {"field": "mulliken", "units": "e"},
    "isotropic-polarizability": {"field": "iso_pol", "units": "Bohr^3"},
    "dipole-moment": {"field": "dip_mom", "units": "Debye"},
    "quadrupole-moment": {"field": "quad_mom", "units": "Debye-Angstrom"},
    "rotational-constants": {"field": "rot_const", "units": "GHz"},
    "homo-energy": {"field": "he", "units": "eV"},
    "lumo-energy": {"field": "le", "units": "eV"},
    "electronic-spatial-extent": {"field": "elec_extent", "units": "Bohr^2"},
    "conformation-identifier": {"field": "conf"},
}

PI_MD = {
    "software": {"value": "Gaussian 09"},
    "method": {"value": "DFT-PBE0"},
    "basis-set": {"value": "6-311G*"},
    "input": {"value": {"SCF": "VeryTight", "Integral": "Grid=UltraFine"}},
}
property_map = {
    "potential-energy": [
        {
            "energy": {"field": "energy", "units": "kcal/mol"},
            "per-atom": {"field": "per-atom", "units": None},
            "_metadata": PI_MD,
        }
    ],
    "atomic-forces": [
        {
            "forces": {"field": "forces", "units": "kcal/mol/A"},
            "_metadata": PI_MD,
        }
    ],
}


def tform(c):
    c.info["per-atom"] = False


DSS = [
    (
        "WS22_acrolein",
        "ws22_acrolein.npz",
        "Configurations of acrolein from WS22. " + DS_DESC,
    ),
    (
        "WS22_alanine",
        "ws22_alanine.npz",
        "Configurations of alanine from WS22. " + DS_DESC,
    ),
    ("WS22_dmabn", "ws22_dmabn.npz", "Configurations of dmabn from WS22. " + DS_DESC),
    (
        "WS22_nitrophenol",
        "ws22_nitrophenol.npz",
        "Configurations of nitrophenol from WS22. " + DS_DESC,
    ),
    (
        "WS22_o-hbdi",
        "ws22_o-hbdi.npz",
        "Configurations of o-hbdi from WS22. " + DS_DESC,
    ),
    ("WS22_sma", "ws22_sma.npz", "Configurations of sma from WS22. " + DS_DESC),
    (
        "WS22_thymine",
        "ws22_thymine.npz",
        "Configurations of o-hbdi from WS22. " + DS_DESC,
    ),
    (
        "WS22_toluene",
        "ws22_toluene.npz",
        "Configurations of toluene from WS22. " + DS_DESC,
    ),
    ("WS22_urea", "ws22_urea.npz", "Configurations of urea from WS22. " + DS_DESC),
    (
        "WS22_urocanic",
        "ws22_urocanic.npz",
        "Configurations of urocanic from WS22. " + DS_DESC,
    ),
]


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

    ds_id = generate_ds_id()
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(atomic_forces_pd)
    for i, (ds_name, ds_reg, ds_desc) in enumerate(DSS):
        ds_id = generate_ds_id()
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=["C", "N", "O", "H"],
            # default_name=f"ws22_{glob}",
            reader=reader_ws22,
            glob_string=ds_reg,
            verbose=True,
            generator=False,
        )

        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                co_md_map=co_md,
                property_map=property_map,
                # generator=False,
                transform=tform,
                verbose=False,
            )
        )

        all_cos, all_dos = list(zip(*ids))

        # cs_ids = []

        # for cs_name, glob, desc in name_glob_desc:
        #     cs_id = client.query_and_insert_configuration_set(
        #         co_hashes=all_cos,
        #         ds_id=ds_id,
        #         name=cs_name,
        #         description=desc,
        #         query={"names": {"$regex": glob}},
        #     )
        #     cs_ids.append(cs_id)

        client.insert_dataset(
            ds_id=ds_id,
            # cs_ids=cs_ids,
            do_hashes=all_dos,
            name=ds_name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=ds_desc,
            resync=True,
            verbose=False,
        )


if __name__ == "__main__":
    main(sys.argv[1:])

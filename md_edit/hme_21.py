"""
author:gpwolfe

Data can be downloaded from:
https://doi.org/10.6084/m9.figshare.19658538.v2

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
splitting into datasets based on file - train, test, val

"""
from argparse import ArgumentParser
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data, generate_ds_id
from colabfit.tools.property_definitions import (
    atomic_forces_pd,
    potential_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/gw_scripts/gw_script_data/hme21")
DATASET_FP = Path().cwd().parent / "data/hme"  # remove
DATASET = "HME21"

SOFTWARE = "VASP 5.4.4"
METHODS = "DFT-PBE"

PUBLICATION = "https://doi.org/10.1038/s41467-022-30687-9"
DATA_LINK = "https://doi.org/10.6084/m9.figshare.19658538.v2"
LINKS = [
    "https://doi.org/10.6084/m9.figshare.19658538.v2",
    "https://doi.org/10.1038/s41467-022-30687-9",
]
AUTHORS = [
    "So Takamoto",
    "Chikashi Shinagawa",
    "Daisuke Motoki",
    "Kosuke Nakago",
    "Wenwen Li",
    "Iori Kurata",
    "Taku Watanabe",
    "Yoshihiro Yayama",
    "Hiroki Iriguchi",
    "Yusuke Asano",
    "Tasuku Onodera",
    "Takafumi Ishii",
    "Takao Kudo",
    "Hideki Ono",
    "Ryohto Sawada",
    "Ryuichiro Ishitani",
    "Marc Ong",
    "Taiki Yamaguchi",
    "Toshiki Kataoka",
    "Akihide Hayashi",
    "Nontawat Charoenphakdee",
    "Takeshi Ibuka",
]
DS_DESC = (
    "The  high-temperature multi-element 2021 (HME21) dataset comprises approximately "
    "25,000 configurations, including 37 elements, used in "
    "the training of a universal NNP called PreFerential Potential (PFP). The "
    "dataset specifically contains disordered and unstable structures, and "
    "structures that include irregular substitutions, as well as varied "
    "temperature and density. "
)
ELEMENTS = [
    "H",
    "Li",
    "C",
    "N",
    "O",
    "F",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
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
    "Mo",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "In",
    "Sn",
    "Ba",
    "Ir",
    "Pt",
    "Au",
    "Pb",
]


def reader(filepath):
    atoms = read(filepath, index=":")
    for i, atom in enumerate(atoms):
        atom.info["name"] = f"{filepath.stem}_{i}"
        atom.info["ref_e"] = sum(
            map(lambda x: REF_E.get(str(x)), atom.arrays["numbers"])
        )
    return atoms


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
    metadata = {
        "software": {"value": SOFTWARE},
        "method": {"value": METHODS},
        "input": {
            "value": {
                "encut": {"value": 520, "units": "eV"},
            }
        }
        # "": {"field": ""}
    }
    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "reference-energy": {"field": "ref_e", "units": "eV"},
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

    dss = (
        ("HME21_test", "hme21_test.xyz", "The test set from HME21. "),
        ("HME21_train", "hme21_train.xyz", "The training set from HME21. "),
        ("HME21_validation", "hme21_val.xyz", "The validation set from HME21. "),
    )
    for name, glob, desc in dss:
        configurations = load_data(
            file_path=DATASET_FP,
            file_format="folder",
            name_field="name",
            elements=ELEMENTS,
            reader=reader,
            glob_string=glob,
            generator=False,
        )
        ds_id = generate_ds_id()
        ids = list(
            client.insert_data(
                configurations,
                ds_id=ds_id,
                property_map=property_map,
                generator=False,
                verbose=True,
            )
        )

        all_co_ids, all_do_ids = list(zip(*ids))

        client.insert_dataset(
            do_hashes=all_do_ids,
            ds_id=ds_id,
            name=name,
            authors=AUTHORS,
            links=[PUBLICATION, DATA_LINK],
            description=f"{desc}{DS_DESC}",
            verbose=True,
            # cs_ids=cs_ids,
        )


REF_E = {
    "0": 0.0,
    "1": -1.11727818,
    "2": 0.00077279,
    "3": -0.32112763,
    "4": -0.04382124,
    "5": -0.46703172,
    "6": -1.37060885,
    "7": -3.12410509,
    "8": -1.90823324,
    "9": -0.62560589,
    "10": -0.01247553,
    "11": -0.26103055,
    "12": -0.01820512,
    "13": -0.30934944,
    "14": -0.8688591,
    "15": -1.88655922,
    "16": -1.07990224,
    "17": -0.37437982,
    "18": -0.02560438,
    "19": -0.25970323,
    "20": -0.06692272,
    "21": -2.1326658,
    "22": -2.53117007,
    "23": -2.69160028,
    "24": -4.49711548,
    "25": -4.56975124,
    "26": -2.15199146,
    "27": -0.71593506,
    "28": 1.40548688,
    "29": 1.19242348,
    "30": -0.01335134,
    "31": -0.27412403,
    "32": -0.78297343,
    "33": -1.70058332,
    "34": -0.89338826,
    "35": -0.26760165,
    "36": -0.02178909,
    "37": -0.26244581,
    "38": -0.09218463,
    "39": -2.30153907,
    "40": -2.27335398,
    "41": -3.1646915,
    "42": -3.36069952,
    "43": -3.43741112,
    "44": -2.4832604,
    "45": -1.50050581,
    "46": -1.47636888,
    "47": -0.20324516,
    "48": -0.01795318,
    "49": -0.24376812,
    "50": -0.67249711,
    "51": -1.42784584,
    "52": -0.72932131,
    "53": -0.21102642,
    "54": -0.01137434,
    "55": 0.0,
    "56": -0.14402131,
    "57": -0.71157278,
    "58": -1.40270727,
    "59": -0.57085281,
    "60": -0.52930911,
    "61": -0.48162496,
    "62": -0.44920156,
    "63": -8.43945271,
    "64": -10.18809297,
    "65": -0.38056492,
    "66": -0.36702509,
    "67": -0.35839768,
    "68": -0.36059071,
    "69": -0.35761699,
    "70": -0.06305004,
    "71": -0.34828495,
    "72": -3.51659686,
    "73": -3.57114815,
    "74": 0.0,
    "75": -4.64054307,
    "76": -2.92546914,
    "77": -1.58831915,
    "78": -0.60979693,
    "79": -0.18656868,
    "80": -0.01255032,
    "81": -0.21375523,
    "82": -0.61471519,
    "83": -1.35719326,
    "84": -0.65033549,
    "85": 0.0,
    "86": -0.00584963,
}
if __name__ == "__main__":
    main(sys.argv[1:])

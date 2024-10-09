"""
author: gpwolfe

File notes
----------
Files have been previously downloaded and unzipped using jarvis-tools to avoid
having this as a dependency.

Properties key:
spg = space group
fund = functional
slme = spectroscopic limited maximum efficiency
encut = ecut/energy cutoff
kpoint_length_unit -> want?
optb88vdw_total_energy (dft_3d)
efg = electric field gradient
mbj_bandgap = band-gap calculated with TBmBJ method


For all JARVIS datasets, if for configuration "cartesian=False", use an
AtomicConfiguration or ase.Atoms object with 'scaled_positions' arg instead of
'positions'.

Keys
['Band_gap_GGA',
 'Band_gap_GGA_optical',
 'Band_gap_HSE',
 'Band_gap_HSE_optical',
 'Direct_or_indirect',
 'Direct_or_indirect_HSE',
 'ICSD_number',
 'Magnetic_ordering',
 'SNUMAT_id',
 'SOC',
 'Space_group_rlx',
 'Structure_rlx',
 'atoms']
"""

import json
import os
from pathlib import Path
from time import time

from ase.atoms import Atoms
from dotenv import load_dotenv
from numpy import isnan

from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.database import (
    DataManager,
    VastDataLoader,
)
from colabfit.tools.property_definitions import band_gap_pd

# Set up data loader environment
load_dotenv()
loader = VastDataLoader(
    table_prefix="ndb.colabfit.dev",
)
access_key = os.getenv("SPARK_ID")
access_secret = os.getenv("SPARK_KEY")
endpoint = os.getenv("SPARK_ENDPOINT")
loader.set_vastdb_session(
    endpoint=endpoint,
    access_key=access_key,
    access_secret=access_secret,
)

# loader.config_table = "ndb.colabfit.dev.co_snu"
# loader.config_set_table = "ndb.colabfit.dev.cs_snu"
# loader.dataset_table = "ndb.colabfit.dev.ds_snu"
# loader.prop_object_table = "ndb.colabfit.dev.po_snu"

loader.config_table = "ndb.colabfit.dev.co_remove_dataset_ids_stage3"
loader.config_set_table = "ndb.colabfit.dev.cs_remove_dataset_ids"
loader.dataset_table = "ndb.colabfit.dev.ds_remove_dataset_ids_stage5"
loader.prop_object_table = "ndb.colabfit.dev.po_remove_dataset_ids_stage4"


print(
    loader.config_table,
    loader.config_set_table,
    loader.dataset_table,
    loader.prop_object_table,
)

DATASET_FP = Path(
    "/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/data/jarvis_json/snumat.json"  # noqa
)

DATASET_NAME = "JARVIS_SNUMAT"
DESCRIPTION = (
    "The JARVIS_SNUMAT dataset is part of the joint automated repository for "
    "various integrated simulations (JARVIS) database. This dataset contains "
    "band gap data for >10,000 materials, computed using a hybrid functional and "
    "considering the stable magnetic ordering. Structure relaxation and band edges "
    "are obtained using the PBE XC functional; band gap energy is subsequently "
    "obtained using the HSE06 hybrid functional. Optical and fundamental band gap "
    "energies are included. Some gap energies are recalculated by including spin-orbit "
    'coupling. These are noted in the band gap metadata as "SOC=true". '
    "JARVIS is a set of tools and collected datasets built to meet current materials "
    "design challenges."
)

DATASET_ID = "DS_1nbddfnjxbjc_0"
DOI = "10.60732/d2b06d5a"
PUBLICATION_YEAR = "2023"

LICENSE = "CC-BY-4.0"

PUBLICATION = "https://doi.org/10.1038/s41597-020-00723-8"
DATA_LINK = "https://ndownloader.figshare.com/files/38521736"
OTHER_LINKS = ["https://jarvis.nist.gov/", "https://www.snumat.com/"]
# LINKS = [
#     "https://doi.org/10.1038/s41597-020-00723-8",
#     "https://jarvis.nist.gov/",
#     "https://www.snumat.com/",
#     "https://ndownloader.figshare.com/files/38521736",
# ]

AUTHORS = [
    "Sangtae Kim",
    "Miso Lee",
    "Changho Hong",
    "Youngchae Yoon",
    "Hyungmin An",
    "Dongheon Lee",
    "Wonseok Jeong",
    "Dongsun Yoo",
    "Youngho Kang",
    "Yong Youn",
    "Seungwu Han",
]

PI_MD = {
    "software": {"value": "VASP"},
    "method": {"field": "method"},
    "spin-orbit-coupling": {"field": "SOC"},
    "fundamental_or_optical": {"field": "fundamental_optical"},
    "keys": {"field": "keys"},
}

PROPERTY_MAP = {
    "band-gap": [
        {
            "energy": {"field": "Band_gap_GGA", "units": "eV"},
            "type": {"field": "direct_indirect", "units": None},
        }
    ],
    "_metadata": PI_MD,
}


CO_KEYS = [
    # "Band_gap_GGA",
    # "Band_gap_GGA_optical",
    # "Band_gap_HSE",
    # "Band_gap_HSE_optical",
    # "Direct_or_indirect",
    # "Direct_or_indirect_HSE",
    "ICSD_number",
    "Magnetic_ordering",
    "SNUMAT_id",
    # "SOC",
    "Space_group_rlx",
    "Structure_rlx",
    # "atoms",
]


CO_METADATA = {key: {"field": key} for key in CO_KEYS}


def reader(fp):
    with open(fp, "r") as f:
        data = json.load(f)
        data = data
    for i, row in enumerate(data):
        atoms = row.pop("atoms")
        elements = [x if x != "D" else "H" for x in atoms["elements"]]
        if atoms["cartesian"] is True:
            config = Atoms(
                positions=atoms["coords"],
                symbols=elements,
                cell=atoms["lattice_mat"],
            )
        else:
            config = Atoms(
                scaled_positions=atoms["coords"],
                symbols=elements,
                cell=atoms["lattice_mat"],
            )
        for band_gap in [
            "Band_gap_GGA",
            "Band_gap_GGA_optical",
            "Band_gap_HSE",
            "Band_gap_HSE_optical",
        ]:
            config_x = config.copy()
            config_x.info["_name"] = (
                f"{fp.stem}__snumatid_{row['SNUMAT_id']}__{i}__{band_gap}"
            )
            config_x.info["band_gap"] = float(row[band_gap])
            config_x.info["keys"] = {band_gap_pd["property-name"]: band_gap}
            if "optical" in band_gap:
                config_x.info["fundamental_optical"] = "optical"
            else:
                config_x.info["fundamental_optical"] = "fundamental"
            if "HSE" in band_gap:
                config_x.info["method"] = "DFT-HSE06"
                config_x.info["direct_indirect"] = row["Direct_or_indirect_HSE"]
            else:
                config_x.info["method"] = "DFT-PBE"
                config_x.info["direct_indirect"] = row["Direct_or_indirect"]
            if config_x.info["direct_indirect"] == "Null":
                config_x.info["direct_indirect"] = "unknown"
            elif config_x.info["direct_indirect"] == "Direct":
                config_x.info["direct_indirect"] = "direct"
            elif config_x.info["direct_indirect"] == "Indirect":
                config_x.info["direct_indirect"] = "indirect"
            else:
                config_x.info["direct_indirect"] = "unknown"
            for key, val in row.items():
                if isinstance(val, str) and val != "na" and len(val) > 0:
                    config_x.info[key.replace(" ", "-")] = val
                elif (
                    isinstance(val, list)
                    and len(val) > 0
                    and any([x != "" for x in val])
                ):
                    config_x.info[key.replace(" ", "-")] = val
                elif isinstance(val, dict) and not all(
                    [v != "na" for v in val.values()]
                ):
                    config_x.info[key.replace(" ", "-")] = val
                elif (isinstance(val, float) or isinstance(val, int)) and not isnan(
                    val
                ):
                    config_x.info[key.replace(" ", "-")] = val
                else:
                    pass
            yield AtomicConfiguration.from_ase(config_x, co_md_map=CO_METADATA)


def main():
    beg = time()
    # loader.zero_multiplicity(ds_id)
    config_generator = reader(DATASET_FP)
    # config_generator = wrapper(DATASET_FP, range)
    dm = DataManager(
        nprocs=1,
        configs=config_generator,
        prop_defs=[band_gap_pd],
        prop_map=PROPERTY_MAP,
        dataset_id=DATASET_ID,
        standardize_energy=True,
        read_write_batch_size=100000,
    )
    print(f"Time to prep: {time() - beg}")
    t = time()
    dm.load_co_po_to_vastdb(loader, batching_ingest=False)
    print(f"Time to load: {time() - t}")
    print("Creating dataset")
    t = time()
    dm.create_dataset(
        loader,
        name=DATASET_NAME,
        authors=AUTHORS,
        publication_link=PUBLICATION,
        data_link=DATA_LINK,
        data_license=LICENSE,
        description=DESCRIPTION,
        publication_year=PUBLICATION_YEAR,
        doi=DOI,
    )
    print(f"Time to create dataset: {time() - t}")
    loader.stop_spark()
    print(f"Total time: {time() - beg}")


if __name__ == "__main__":
    # import sys

    # range = (int(sys.argv[1]), int(sys.argv[2]))
    main()

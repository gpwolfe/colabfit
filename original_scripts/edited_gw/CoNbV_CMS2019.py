#!/usr/bin/env python
# coding: utf-8
from argparse import ArgumentParser
from pathlib import Path
import sys

from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.configuration import AtomicConfiguration

client = MongoDatabase(
    "colabfit-12-11-22",
    configuration_type=AtomicConfiguration,
    nprocs=1,
    drop_database=False,
)

DATASET_FP = Path("/persistent/colabfit_raw_data/colabfit_data/data/")
DATASET = "CoNbV_CMS2019"

LINKS = [
    "https://www.sciencedirect.com/science/article/pii/S0927025618306372?via%3Dihub",
    "https://gitlab.com/kgubaev/accelerating-high-throughput-searches-for-new-alloys"
    "-with-active-learning-data",
]
AUTHORS = ["K. Gubaev", "E. V. Podryabinkin", "G. L. W. Hart", "A. V. Shapeev"]
DS_DESC = (
    "This dataset was generated using the following active "
    "learning scheme: 1) candidate structures were relaxed by a partially-"
    "trained MTP model, 2) structures for which the MTP had to perform "
    "extrapolation were passed to DFT to be re-computed, 3) the MTP was "
    "retrained, including the structures that were re-computed with DFT, 4)"
    " steps 1-3 were repeated until the MTP no longer extrapolated on any of "
    "the original candidate structures. "
    "The original candidate structures for this dataset included "
    "about 27,000 configurations that were bcc-like and close-packed (fcc, "
    "hcp, etc.) with 8 or fewer atoms in the unit cell and different "
    "concentrations of Co, Nb, and V."
)


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

    configurations = load_data(
        file_path=DATASET_FP / "gubaev/CoNbV/train.cfg",
        file_format="cfg",
        name_field=None,
        elements=["Co", "Nb", "V"],
        default_name=DATASET,
        verbose=True,
        generator=False,
    )

    # client.insert_property_definition('potential-energy.json')
    # client.insert_property_definition('atomic-forces.json')
    # client.insert_property_definition('cauchy-stress.json')

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"field": "per-atom", "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                },
            }
        ],
        "cauchy-stress": [
            {
                "stress": {"field": "virial", "units": "GPa"},
                "_metadata": {
                    "software": {"value": "VASP"},
                },
            }
        ],
    }

    def tform(c):
        c.info["per-atom"] = False

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        resync=True,
        verbose=True,
    )


"""
client.apply_labels(
    dataset_id=ds_id,
    collection_name='configurations',
    query={'hash': {'$in': all_co_ids}},
    labels='active_learning',
    verbose=True
)

ds_id = 'DS_911472907883_000'

dataset = client.get_dataset(ds_id, resync=True, verbose=True)['dataset']

for k,v in dataset.aggregated_info.items():
    print(k,v)

dataset.aggregated_info['property_fields']
fig = client.plot_histograms(dataset.aggregated_info['property_fields'],
                            ids=dataset.property_ids, method='matplotlib')

client.dataset_to_markdown(
    ds_id=ds_id,
    base_folder='/colabfit/markdown/'+dataset.name,
    html_file_name='README.md',
    data_format='mongo',
    data_file_name=None,
)
"""

if __name__ == "__main__":
    main(sys.argv[1:])
